"""Connectors to the FB Offline Conversions API"""

import os
import sys
import time
import requests
import json
import logging
import math
import datetime
from urllib.request import URLopener
from concurrent.futures import ThreadPoolExecutor
from threading import current_thread
from multiprocessing import current_process
from functools import partial
import pandas as pd
from pathlib import Path

from conv_api.endpoints import FB_Endpoints, API
from utils.hive.hive_query import hive_query
from utils.common import create_week_boundaries

# TODO: Parametrize hardcoded options in `request_run` - Needs FB to update API
# TODO: Update deprecated download approach in `download_report_files`


def create_event_set(access_token, business_id, set_name, description):
    """Step #1: Create an empty Offline Event Set"""

    url = FB_Endpoints.get_url(API.CREATE_EVENT_SET, business_id = business_id)
    data = {
        'access_token': access_token,
        'name': set_name,
        'description': description,
        'is_mta_use': 'true'
    }
    logging.debug("Create event set call:\n%s\n\n%s", url, data)
    r = requests.post(url, data=data, timeout=100)
    r_json = r.json()
    logging.debug("Create event set response:\n%s", r_json)
    r.raise_for_status()
    if 'id' in r_json:
        return r_json['id']
    else:
        raise RuntimeError("Failed to create Event Set")


def attach_event_set(access_token, event_set_id, business_id, account_id):
    """Step #2: Associate the Offline Event Set with the corresponding Dummy Ad Account"""

    url = FB_Endpoints.get_url(API.ATTACH_EVENT_SET, event_set_id = event_set_id)
    data = {
        'access_token': access_token,
        'business': business_id,
        'account_id': account_id
    }
    logging.debug("Attach event set call:\n%s\n\n%s", url, data)
    r = requests.post(url, data=data, timeout=1000)
    r.raise_for_status()
    logging.debug("Attach event set response:\n%s", r.json())
    return r.json()


def chunker(seq, size):
    """Generates chunks of an inputted sequence"""
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def upload_chunk(access_token: str, event_set_id: str, tag: str, staging_loc: str,  failed_dir:str, timeout: int, dry_run: bool, df: pd.DataFrame):
    """Creates and sends the POST request to upload a chunk of data"""
    if df is None: return
    # Generate events JSON
    event_list = []
    for row in df.itertuples():
        if isinstance(row.custom_data_cookie_ids, str) and len(row.custom_data_cookie_ids) > 1:
            cookie_ids = row.custom_data_cookie_ids.split('|')
        else:
            cookie_ids = []
        if isinstance(row.custom_data_ext_ids, str) and len(row.custom_data_ext_ids) > 1:
            ext_ids = row.custom_data_ext_ids.split('|')
        else:
            ext_ids = []
        event_data = {
            "match_keys": {"extern_id": row.match_keys_extern_id},
            "event_name": "Other",
            "event_time": row.event_time,
            # "order_id": "NsrConversionId1",
            "custom_data": {
                "type": row.custom_data_type,
                "hhid": row.match_keys_extern_id,
                "cookie_ids": cookie_ids,
                "ext_ids": ext_ids
            }
        }
        event_list.append(event_data)

    # Generate request
    url = FB_Endpoints.get_url(API.UPLOAD, event_set_id = event_set_id)
    data = {
        'access_token': access_token,
        'upload_tag': tag,
        'data': json.dumps(event_list)
    }
    logging.debug("URL call: %s", url)
    logging.debug("Call data head: \n%s", json.dumps(data, indent=2)[:500])
    logging.debug("Call data tail: \n%s", json.dumps(data, indent=2)[-200:])
    logging.debug("First events:\n%s", event_list[:10])
    if dry_run:
        return len(event_list)  # data

    # Send request to API
    def save_failed(df, staging_loc , failed_dir = 'failed'):
        if staging_loc is not None:
            staging_loc = os.path.join(staging_loc, failed_dir)
            os.makedirs(staging_loc, exist_ok=True)
        else:
            failed_staging_dir = tempfile.TemporaryDirectory()
            staging_loc = os.path.join(failed_staging_dir.name, failed_dir)
        
        path = os.path.join(staging_loc,
            "failed_{}_{}_{:.0f}.csv.gz".format(
                current_process().ident,
                current_thread().ident,
                1000 * time.time()
            )
        )
        df.to_csv(
            path,
            index=False,
            compression='gzip')

    try:
        r = requests.post(url, data=data, timeout=timeout)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error while uploading {tag}: {errh}")
        save_failed(df, staging_loc, failed_dir)
        return len(event_list), data
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting while uploading {tag}: {errc}")
        save_failed(df, staging_loc, failed_dir)
        return len(event_list), data
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error while uploading {tag}: {errt}")
        save_failed(df, staging_loc, failed_dir)
        return len(event_list), data
    except requests.exceptions.RequestException as err:
        logging.error(f"Other exception while uploading {tag}: {err}")
        save_failed(df, staging_loc, failed_dir)
        return len(event_list), data
    return len(event_list), r.json()


def upload_events(table: str, access_token: str, event_set_id: str,
                  start: str, end: str, tag: str, config: str, staging_loc=None, failed_dir = 'failed', timeout= 10*60, dry_run: bool=False):
    """Step #4: Populate the Offline Event Set"""

    # Pull table data in chunks, day by day
    results = []
    for d in pd.date_range(start=start, end=end):
        the_date = d.date()
        logging.info("Processing date %s, within tag %s", the_date, tag)
        query_count = f"SELECT COUNT(*) AS nrows FROM {table} WHERE TO_DATE(from_unixtime(CAST(event_time AS BIGINT))) = '{the_date}';"
        logging.info("query_count: %s", query_count)
        num_rows = hive_query(query_count,  config=config, staging_dir=staging_loc).loc[0, 'nrows']
        logging.info("%s rows in date %s", num_rows, the_date)
        if num_rows is not None and num_rows == 0:
            continue
        chunksize = 1900
        logging.info("Using a chunksize of %s resulting in %s batches", chunksize, math.ceil(num_rows/chunksize))
        query = f"SELECT * FROM {table} WHERE TO_DATE(from_unixtime(CAST(event_time AS BIGINT))) = '{the_date}';"
        logging.info("Get data query: %s", query)
        # Partially evaluate upload function
        partial_upload_chunk = partial(upload_chunk, access_token, event_set_id, tag, staging_loc, failed_dir, timeout, dry_run)
        # Pull data in chunks and pass them to parallel threads
        chunked_df = hive_query(query, config=config, staging_dir=staging_loc, nrows=None, debug=False, chunksize=chunksize)
        with ThreadPoolExecutor(max_workers=5) as executor:
            results_date = executor.map(partial_upload_chunk, chunked_df, timeout=60*60)
        results += list(results_date)
    return results


def upload_events_path(path: str, access_token: str, event_set_id: str,
                       start: str, end: str, tag: str, config: str, staging_loc=None, failed_dir = 'failed', timeout= 10*60, dry_run: bool=False):
    """Step #4: Populate the Offline Event Set"""

    chunksize = 1900
    columns = ["match_keys_extern_id", "event_time", "actuuid", "custom_data_cookie_ids",
               "custom_data_ext_ids", "custom_data_type"]
    results = []
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        logging.info("Loading file: %s", file_path)
        # Partially evaluate upload function
        partial_upload_chunk = partial(upload_chunk, access_token, event_set_id, tag, staging_loc, failed_dir, timeout, dry_run)
        # Pull data in chunks and pass them to parallel threads
        chunked_df = pd.read_csv(file_path, header=None, names=columns, chunksize=chunksize)
        with ThreadPoolExecutor(max_workers=5) as executor:
            results_file = executor.map(partial_upload_chunk, chunked_df, timeout=60*60)
        results += list(results_file)
    return results


def get_stats(access_token, event_set_id):
    """Get upload status for the bucket"""
    url = FB_Endpoints.get_url(API.UPLOAD_STATUS, event_set_id = event_set_id)
    logging.debug("get_stats is callig....\n url: {}".format(url))
    params = {
        'access_token': access_token
    }
    r = requests.get(url, params, timeout=100)
    return r.json()


def get_reports_urls(access_token, business_id, pixel_id, request_id, **kwargs):
    """Get URLs for downloading results from FB"""
    logging.debug("get_reports_urls call....")
    url = FB_Endpoints.get_url(API.DOWNLOAD_REPORT_URLS, business_id = business_id)
    params = {
        'fields': json.dumps(["download_urls", "metadata"]),
        'report_type': "third_party_mta_report",
        'access_token': access_token,
        'filters': json.dumps(
            [
                {"key": "account",
                 "value": pixel_id  # "734394220033893"
                 },
                {"key": "name",
                 "value": request_id,
                 "comparison": "in"
                 }
            ]
        ),
    }
    for arg in kwargs:
        params[arg] = kwargs[arg]
    logging.debug("Get reports URL: {}".format(url))
    logging.debug("Get reports params: {}".format(params))
    urls = {}
    r = requests.get(url, params, timeout=100)
    r.raise_for_status()
    r_json = r.json()
    logging.debug(json.dumps(r_json, indent=2))
    break_next = False
    while True:
        for report in r_json['data']:
            if 'name' in report['metadata']:
                file_name = report['metadata']['name'] + ".csv.gpg.gz"
            else:
                file_name = report['download_urls'][0]
            urls[file_name] = {
                'url': report['download_urls'][0],
                'file_size': int(report['metadata']['file_size']) if 'file_size' in report['metadata'] else 0
            }
        logging.debug("urls length: {}".format(len(urls)))
        if break_next or 'paging' not in r_json or 'next' not in r_json['paging']:
            break
        next_url = r_json['paging']['next']
        logging.debug(f"Next url = {next_url}")
        r = requests.get(next_url, timeout=1000)
        r.raise_for_status()
        r_json = r.json()
        if 'next' not in r_json['paging']:
            break_next = True
        logging.debug("Get reports URL Response: {}".format(urls))
    return urls


def download_report_files(urls, path="./", check_sizes=True):
    """Downloads files from urls"""
    logging.info("=============================================")
    logging.info("Downloading files..............")
    logging.info("=============================================")
    urlopener = URLopener()
    for file in urls:
        file_path = os.path.abspath(os.path.join(path, file))
        urlopener.retrieve(urls[file]['url'], file_path)
        # Check that the file downloaded has the size specified by the API
        file_size = os.path.getsize(file_path)
        if check_sizes and file_size != urls[file]['file_size']:
            raise Exception(
                "Downloaded file size ({}) is not equal to reported size from API ({})".format(
                    file_size,
                    urls[file]['file_size']
                )
            )
        else:
            logging.info(f"Downloaded {file}, size {file_size}")


def request_run(tag, access_token, business_id, start_date, end_date):
    """Sends signal to FB that an upload tag is ready to run"""
    
    logging.debug("===Upload Evalution has been started===")
    url = FB_Endpoints.get_url(API.UPLOAD_EVALUTION, business_id = business_id)
    logging.debug("Request run URL: {}".format(url))

    responses = []
    for week in create_week_boundaries(start_date, end_date):
        start = week[0].strftime('%Y-%m-%d')
        end = week[1].strftime('%Y-%m-%d')
        params = {
            'upload_tag': tag,
            'conversion_start_date': start,
            'conversion_end_date': end,
            'lookback_window': 'DAYS60',
            'match_universe': 'FULL',
            'timezone': 'TZ_AMERICA_LOS_ANGELES',
            'aggregation_level': 'WEEKLY',
            'access_token': access_token
        }
        logging.info("Request run params: {}".format(params))
        r = requests.post(url, params, timeout=100)
        r.raise_for_status()
        r_json = r.json()
        r_json['date_start'] = start
        r_json['date_end'] = end
        logging.info("response: {}".format(r_json))
        responses.append(r_json)
    return responses


def get_request_run_status(access_token, request_id):
    """Get upload status for the bucket"""
    url = FB_Endpoints.get_url(API.UPLOAD_EVALUTION_STATUS, request_id = request_id)
    logging.debug("get_request_run_status call \nurl: {}".format(url))
    params = {
        'access_token': access_token,
        'fields' : "upload_tag,event_status"
    }
    r = requests.get(url, params, timeout=100)
    logging.info("response: {}".format(r.json()))
    return r.json()
