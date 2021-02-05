import os, sys, glob
import argparse
import logging
import time
import json
import tempfile
import shutil
import traceback
import pandas as pd
from datetime import datetime
from pathlib import Path

from utils.common import retry, configure_logging, is_numeric
from utils.bash_utils.shell_cmd_util import gunzip, decryption, import_gpg_key
from utils.s3.s3_utils import s3_put, s3_put_file, s3_put_files_recursive
from conv_api.conv_api_conn import upload_events, upload_events_path, create_event_set, attach_event_set, \
                                    get_reports_urls, download_report_files, request_run, get_request_run_status


def create_parser(parent_parser=None):
    if parent_parser is None:
        parser = argparse.ArgumentParser(description='Uploads path records to FB using the Offline Conversions API')
    else:
        parser = argparse.ArgumentParser(parents=[parent_parser], 
                                         description='Uploads path records to FB using the Offline Conversions API', 
                                         conflict_handler='resolve')
    parser.add_argument('--config',
                        dest='config',
                        action='store',
                        required=True,
                        type=str,
                        help='Path to the platform_config.xml file for the client')
    parser.add_argument('--table',
                        dest='table',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=False,
                        default=None,
                        help='Name of the table with records to be uploaded. Either table or path must be passed')
    parser.add_argument('--path',
                        dest='path',
                        metavar='/path/to/folder',
                        action='store',
                        type=str,
                        required=False,
                        default=None,
                        help='Path to folder with files to be uploaded. Either table or path must be passed')
    parser.add_argument('--start-date',
                        dest='start_date',
                        metavar='yyyy-mm-dd',
                        action='store',
                        required=True,
                        type=str,
                        help='start date')
    parser.add_argument('--end-date',
                        dest='end_date',
                        metavar='yyyy-mm-dd',
                        action='store',
                        required=True,
                        type=str,
                        help='end date')
    parser.add_argument('--access-token',
                        dest='access_token',
                        action='store',
                        required=True,
                        type=str,
                        help='Access token for API')
    parser.add_argument('--tag-suffix',
                        dest='tag_suffix',
                        action='store',
                        required=True,
                        type=str,
                        help='Suffix to be added to upload tag')
    parser.add_argument('--event-set-id',
                        dest='event_set_id',
                        action='store',
                        required=False,
                        type=str,
                        default=None,
                        help='If not provided, a new event set will be created')
    parser.add_argument('--business-id',
                        dest='business_id',
                        action='store',
                        required=True,
                        type=str,
                        help='Required if an event-set-id is not provided')
    parser.add_argument('--event-set-name',
                        dest='set_name',
                        action='store',
                        required=True,
                        type=str,
                        default=None,
                        help='Free from event set name. Required if an event-set-id is not provided')
    parser.add_argument('--event-set-description',
                        dest='description',
                        action='store',
                        required=True,
                        type=str,
                        default=None,
                        help='Free-form event set description. Required if an event-set-id is not provided')
    parser.add_argument('--account-id',
                        dest='account_id',
                        action='store',
                        required=True,
                        type=str,
                        default=None,
                        help='Required if an event-set-id is not provided')
    parser.add_argument('--pixel-id',
                        dest='pixel_id',
                        action='store',
                        required=True,
                        type=str,
                        help='Client Pixel ID in FB')
    parser.add_argument('--tpre-staging-path',
                        dest='staging_path',
                        action='store',
                        required=True,
                        type=str,
                        help='Path where to save report files')
    parser.add_argument('--origin',
                        dest='origin',
                        action='store',
                        required=False,
                        default="cmd",
                        type=str,
                        help='From where FBRE initiated')
    parser.add_argument('--config-loc',
                        dest='config_inputs_loc',
                        action='store',
                        required=True,
                        type=str,
                        help='Project config inputs location path')
    parser.add_argument('--upload-events-data-path',
                        dest='upload_event_data_path',
                        action='store',
                        required=False,
                        default=None,
                        type=str,
                        help='Path where to save report files')
    parser.add_argument('--client-warehouse-path',
                        dest='client_warehouse_path',
                        action='store',
                        required=True,
                        type=str,
                        help='S3 client warehouse location')
 
    parser.add_argument('--dry-run',
                        dest='dry_run',
                        action='store',
                        choices=[True, False],
                        required=False,
                        default=False,
                        type=bool,
                        help='If passed, no API calls are made.')
    parser.add_argument('--log',
                        dest='loglevel',
                        action='store',
                        choices=['WARNING', 'INFO', 'DEBUG'],
                        required=False,
                        default='INFO',
                        type=str,
                        help='logging level')

    return parser



def validate_input(args):

    # Validating inputs
    if not args.event_set_id:
        assert args.set_name is not None, "Event set name must be provided if event set ID isn't"
        assert args.description is not None, "Event set description must be provided if event set ID isn't"
        assert args.business_id is not None, "Business ID must be provided if event set ID isn't"
        assert args.account_id is not None, "Account ID must be provided if event set ID isn't"
        assert args.table is not None or args.path is not None, "Either table or path should be provided"
    assert is_date(args.start_date), "start_date is not in the correct date format (yyyy-mm-dd)"
    assert is_date(args.end_date), "end_date is not in the correct date format (yyyy-mm-dd)"
    assert args.table is not None or args.path is not None, "Neither path nor table supplied"



def get_event_set_id(access_token, business_id, account_id, set_name, description, config_loc):
    '''
        Find  Offline Event Set id for a project of a client
    '''
    
    logging.info("================= Find Offline event set  ================\n")
    sys_def_file_name = 'tpre_system_def.json'
    sys_def_file = os.path.join(config_loc, sys_def_file_name)
    if not (os.path.exists(config_loc) and os.path.isdir(config_loc) and os.path.isfile(sys_def_file)):
        raise Exception('config inputs loc or tpre_system_def.json is not valid')
     
    with open(sys_def_file, 'r+') as sys_def_json:       
        sys_def_dict : dict  = json.load(sys_def_json)
        if sys_def_dict.__contains__('event_set_id'):
            event_set_id = sys_def_dict['event_set_id']
            if(is_numeric(event_set_id)):
                logging.info("Get event set id: {} (from tpre_system_def.json)".format(event_set_id))
                return event_set_id

        event_set_id = create_event_set_id(access_token, business_id, account_id, set_name, description)
        sys_def_dict : dict = {'event_set_id': event_set_id}
        sys_def_json.seek(0)       
        json.dump(sys_def_dict, sys_def_json, indent=4)
        sys_def_json.truncate()
    return event_set_id



def create_event_set_id(access_token, business_id, account_id, set_name, description):
    '''
        Create the Offline Event Set 
    '''

    logging.info("=========== New Offline event set creation has started =============\n")
    
    event_set_id = create_event_set(access_token, business_id, set_name, description)
    logging.info("Created event set id: {}\n".format(event_set_id))
    logging.info("========= Attaching event set with ad account has started ===========\n")
    attach_event_set(access_token, event_set_id, business_id, account_id)
    logging.info("====== Event set id with ad account has attached successfully ======\n")
    return event_set_id



def upload(tag, config, access_token, business_id, account_id, table, set_name, description, start_date, end_date,  
        path = None, event_set_id=None, dry_run=False, staging_loc = None, failed_dir = "upload_failures", 
        max_attempts = 3, tpre_started_on=int(time.time())):

    '''
        Populate the Offline Event Set with upload tag
    '''

    logging.info("\n=========== Upload for offline event set has been started =============\n")
    logging.info("Event set ID: {}".format(event_set_id))
    logging.info("Upload Tag: {}".format(tag))
    # Upload data
    results = {}
    upload_failed_dir = '{}/failed_on_attempt_{}'.format(failed_dir, 1)
    logging.info("Uploading {}, {}, {}".format(tag, start_date, end_date))
    if table is not None:
        results[tag] = upload_events(
            table, access_token,
            event_set_id, start_date, end_date,
            tag, config, staging_loc, upload_failed_dir, dry_run=dry_run)
    else:
        if not os.path.isdir(path):
            raise Exception("Folder not found: " + path)
        logging.info("Upload attempt: {} from src location : {}".format(1, path))
        results[tag] = upload_events_path(
            path, access_token,
            event_set_id, start_date, end_date,
            tag, config, staging_loc, upload_failed_dir, dry_run=dry_run)

    # Writing results to a file
    persist_upload_result(results, staging_loc, tpre_started_on=tpre_started_on)
    failure_dir_path = '{}/{}'.format(staging_loc,upload_failed_dir)
    logging.info("Uploaded! in attempt: {} found upload fails at dir : [staging dir]/{}".format(1, upload_failed_dir))
    
    attempt_num = 2
    
    while(os.path.exists(failure_dir_path) 
            and os.path.isdir(failure_dir_path) 
            and len(os.listdir(failure_dir_path)) > 0
            and attempt_num <= max_attempts):
        
        logging.info("Upload attempt: {} from src location : {}".format(attempt_num, failure_dir_path))
        upload_failed_dir = '{}/failed_on_attempt_{}'.format(failed_dir, attempt_num)
        
        results[tag] = upload_events_path(
            failure_dir_path, access_token,
            event_set_id, start_date, end_date,
            tag, config, staging_loc, upload_failed_dir, dry_run=dry_run)
        failure_dir_path = '{}/{}'.format(staging_loc,upload_failed_dir)
        persist_upload_result(results, staging_loc, attempt_num, tpre_started_on=tpre_started_on)
        logging.info("Uploaded! in attempt: {} found upload fails at dir : [staging dir]/{}".format(attempt_num, upload_failed_dir))
        attempt_num = attempt_num + 1
        
        

def persist_upload_result(results, staging_loc, attempt_num = 1, result_dir="conversions_upload_results", 
                          tpre_started_on=int(time.time())):
    if staging_loc is not None:
        staging_loc = os.path.join(staging_loc, '{}/res_{}'.format(result_dir, tpre_started_on))
        os.makedirs(staging_loc, exist_ok=True)
    else:
        response_staging_dir = tempfile.TemporaryDirectory()
        staging_loc = os.path.join(response_staging_dir.name, result_dir)

    logging.info("Staging dir for upload results: {}".format(staging_loc))
    
    response_staging_file = os.path.join(staging_loc, '{}_conversions_upload_output_{:.0f}.json'.format(attempt_num, time.time()))
    with open(response_staging_file, 'w') as outfile:
        json.dump(results, outfile, indent=2)



def upload_evalution(tag, config, access_token, business_id, start_date, end_date, 
                     s3_loc, staging_loc, tpre_started_on=int(time.time()), **kwargs ):
    '''
        Request run for evalutating the UPLOAD.
    '''

    logging.info("=========== Request Run has started =============")
    logging.info("Upload Evalution for tag: {}".format(tag))
    # Upload Event evalution
    responses = request_run(tag, access_token, business_id, start_date, end_date)
    #request_ids = [r['id']  for r in responses]
    return save_request_id_mapping_to_s3(responses, config, s3_loc, staging_loc, tpre_started_on)



def save_request_id_mapping_to_s3(responses, config, s3_loc, staging_loc, tpre_started_on=int(time.time())):
    '''
        Persisting Request run result in S3.
    '''

    logging.info("=========== Persisting Request run result to s3 =============")
    request_date_mapping = pd.DataFrame(data=responses, columns=['id', 'date_start', 'date_end'])
    request_date_mapping.rename(columns={'id': 'request_id'}, inplace=True)
    #TODO send to S3
    if s3_loc is not None: 
        s3_loc_path = "{}/tpre_results/request_id_mapping/".format(s3_loc)
    if staging_loc is not None:
        staging_loc = os.path.join(staging_loc,"request_run_result")
        os.makedirs(staging_loc, exist_ok=True)
        logging.info("Staging dir for request run: "+ staging_loc)
    
    request_date_mapping['created_timestamp'] = tpre_started_on
    s3_put(request_date_mapping, config, "request_id_mapping", s3_location=s3_loc_path, staging_location = staging_loc)
    request_ids = request_date_mapping['request_id'].tolist()
    logging.info("Response Upload Evalution [Request_ids]: "+ str(request_ids))
    return request_ids



def upload_evalution_status(access_token, request_id):
    '''
        Get request run status.
    '''
    try:
        status_json = get_request_run_status(access_token, request_id)
        return status_json is not None and status_json['event_status'] == 'COMPLETED'
    except Exception as e:
        return False
                                         


def get_report_urls(access_token, business_id, pixel_id, request_ids, max_cnt = 4, retry_max = 30):
    '''
        Get urls for report of request run from Facebook.
    '''

    logging.info("=========== Get Generate Report Urls =============")
    urls = {}
    for rid in request_ids:
        evalution_success_status = False
        retry = retry_max
        count = 1
        
        while  ((not evalution_success_status  
                 or len(get_reports_urls(access_token, business_id, pixel_id, rid)) == 0)
                and count <= max_cnt):
            logging.info("\n\n--------------------------------------------")
            logging.info("checking evalution success status for request id: {}".format(str(rid)))
            waiting_period = count*count*120
            logging.info("Waiting: {} (in seconds).........".format(str(waiting_period)))
            time.sleep(waiting_period)
            count = count + 1
            evalution_success_status = upload_evalution_status(access_token, rid)
            if retry >= 0 and count > max_cnt:
                retry = retry - 1
                count = 1
                logging.info("\n\n--------------------------------------------")
                logging.info("\nRetry: {} polling request run for completed\n".format(str(retry_max - retry))) 
        logging.info("***** Working on: {}".format(str(rid)))
        #time.sleep(60*60)
        #logging.info("***** Additional Wait for: {} seconds for prepare reports.".format(str(60*60)))
        if not evalution_success_status or len(get_reports_urls(access_token, business_id, pixel_id, rid)) == 0:
            logging.info("Request id {} has completed but still no reports available.".format(rid))
            raise Exception("No Reports available for request id {}".format(rid))
        urls.update(get_reports_urls(access_token, business_id, pixel_id, rid))
        logging.info("Response report urls: {}".format(str(urls)))
    return urls



def download_report(urls, destpath, **kwargs ):
    '''
        Download the reports from given urls to given path.
    '''

    logging.info("=========== Downloading Reports from Urls =============")
    os.makedirs(destpath, exist_ok=True)
    # Download reports
    download_report_files(urls, path=destpath, check_sizes=False)



def gunzip_decrypt(destpath):
    '''
        Gunzip and decrypt the files on given destpath.
        destpath : directory path where encrypted and gunzip input files located.
    '''

    logging.info("=========== Gunzip and Decryption =============")
    gunzip(destpath)
    import_gpg_key()
    decryption(destpath, "decrypt")



def copy_fb_report_to_s3(config, destpath, s3_loc=None):
    cohort_size = '100'
    schema =  {
        'membership': ('pixel_id', 'hh_id', 'cohort_number', 'request_id'),
        'metric': ('pixel_id', 'cohort_number', 'click_value', 'imp_value', 'request_id'),
        'summary': ('pixel_id', 'account_id', 'campaign_id', 'adset_id', 'ad_id', 'click_count', 'imps_count', 'request_id'),
        'taxonomy': ('pixel_id', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name', 'click_or_imp', 'all_events', 'successful_events', 'request_id')
    }
    
    for ftype in schema:
        logging.info("*** Working on: {}".format(ftype))
        s3_loc_path = None
        if s3_loc is not None:
            s3_loc_path = s3_loc+"/tpre_results4/"+ftype+"/"
        path = os.path.join(destpath, "/decrypt")
        search_path = os.path.join(path, "*{}_{}*.csv".format(ftype, cohort_size))
        logging.info(search_path)
        files = glob.glob(search_path)
        logging.info(files)
        for file_path in files:
            #TODO send to S3
            s3_put(config, file_path, s3_loc_path)



def dump_fb_report_to_s3(config, destpath, s3_loc=None):
    '''
        Structuring and re-arranging decypted reports and dumping to s3.
    '''
    
    logging.info("=========== Persisting Evaluted report to S3 =============")
    cohort_size = '100'
    schema =  {
        'membership': ('pixel_id', 'hh_id', 'cohort_number', 'request_id'),
        'metric': ('pixel_id', 'cohort_number', 'click_value', 'imp_value', 'request_id'),
        'summary': ('pixel_id', 'account_id', 'campaign_id', 'adset_id', 'ad_id', 'click_count', 'imps_count', 'request_id'),
        'taxonomy': ('pixel_id', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name', 'click_or_imp', 'all_events', 'successful_events', 'request_id')
    }
    path = os.path.join(destpath, "decrypt") 
    s3_staging = os.path.join(destpath, "s3_staging") 
    os.makedirs(s3_staging, exist_ok=True)
    logging.info(s3_staging)
    for ftype in schema:
        os.makedirs(os.path.join(s3_staging, ftype), exist_ok=True)
    for ftype in schema:
        logging.info("=================================================")
        logging.info("*** Working on: {}".format(ftype))
        if ftype not in ("taxonomy",):
            search_path = os.path.join(path, "*{}*{}.csv".format(ftype,cohort_size))
        else:
            search_path = os.path.join(path, "*{}*.csv".format(ftype))
        logging.info(search_path)
        files = glob.glob(search_path)
        logging.info(files)
        ftype_s3_staging = os.path.join(s3_staging, ftype)
        logging.info(ftype_s3_staging)
        
        for file_path in files:
            shutil.copy(file_path, ftype_s3_staging, follow_symlinks=False)
        s3_loc_path = "{}/tpre_results/{}/".format(s3_loc,ftype)
        logging.info(s3_loc_path)
        s3_put_files_recursive(config, ftype_s3_staging, s3_loc_path)
        


def facebook_remote_execution(access_token, account_id, pixel_id, business_id, table, 
                            set_name, description, event_set_id, start_date, end_date, tag, 
                            config, upload_file_path, report_destpath, config_loc,
                            client_warehouse_path, origin, tpre_started_on=int(time.time()), dry_run=False):
    
    ''' 
       Facebook remote Execution
    
    '''

#     # ======Below commented section used to skip attach_event_set for CTAM================
#    event_set_id = 2197844467193741    # CTAM
#     #====================================================================================
    
    logging.info("=========== Welcome to Facebook Remote execution =============")

    ''' Get Event Set Id '''
    if not is_numeric(event_set_id):
        event_set_id = get_event_set_id(access_token, business_id, account_id, set_name, description, config_loc)
    logging.info("event set id: {}\n".format(event_set_id))
    
    upload_tag = '{}_{}_tag'.format(tag, str(tpre_started_on))

    ''' Upload Events '''
    upload(upload_tag, config, access_token, business_id, account_id, table, set_name, description, 
           start_date, end_date, upload_file_path, event_set_id = event_set_id, staging_loc = report_destpath, 
           tpre_started_on = tpre_started_on)
    
    ''' Request run for uploaded events Evalution '''
    request_ids = upload_evalution(upload_tag, config, access_token, business_id, start_date, end_date, 
                                   client_warehouse_path, report_destpath, tpre_started_on)
    
    
#     # ======Below commented section used to test below functionality after evalution======
#     pixel_id = '128900881029137'
#     request_ids = ['848478348835361']
#    request_ids = [1267331710097605] # Cricket
#     #====================================================================================
    
    if request_ids is None or len(request_ids) == 0:
        return
    
    if origin is not None and origin.lower() == 'modeling': retry_max = 48
    elif origin is not None and origin.lower() == 'attribution': retry_max = 24
    else: retry_max = 24
    
    urls = get_report_urls(access_token, business_id, pixel_id, request_ids, retry_max = retry_max)

    if urls is None or len(urls) == 0:
        return
    
    ''' Download Evaluted Reports '''
    report_download_path = os.path.join(report_destpath, "downloads")
    download_report(urls, report_download_path)
    
    ''' unzip and decrypt reports '''
    gunzip_decrypt(report_download_path)
    
    ''' Dumping reports to s3 on client warehouse '''
    dump_fb_report_to_s3(config, report_download_path, client_warehouse_path)
    logging.info("=========Facebook Remote execution has finished =========")

    


def cleanup(number_of_days, path, pattern ="{}_{}*".format('fb','staging'), dry_run = True):
    
    """
    Removes dirs from the passed in path and matched pattern that are older than or equal 
    to the number_of_days
    """
    time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
    search_path = os.path.join(path, pattern)
    files = glob.glob(search_path)

    logging.info("Search pattern for fb_staging files: {}".format(search_path))
    logging.info("All the pattern matched files are below:")
    for f in files:
        stat = os.stat(f)
        logging.info('{} : {}'.format(datetime.fromtimestamp(stat.st_mtime), f))
    logging.info('--------------------------------------------------------')
    for f in files:
        stat = os.stat(f)
        
        #if stat.st_mtime <= 1558892792:
        if stat.st_mtime <= time_in_secs:
            logging.info("Staging dir is going to delete: {}".format(f))
            if not dry_run:
                try:
                    shutil.rmtree(f)
                    logging.info("Deleted: {}".format(f))
                except Exception as e:
                    #ex_type, ex_value, ex_traceback = sys.exc_info()
                    logging.error(str(e))
    logging.info('Cleanup completed!')
    


def main(tp='FB', parent_parser=None, tpre_started_on=int(time.time()), 
         log_file=None):
    '''
        Entry point for FB remote execution
    '''
    parser = create_parser(parent_parser)
    args = parser.parse_args()
    access_token = args.access_token
    account_id = args.account_id
    pixel_id = args.pixel_id
    business_id = args.business_id
    table = args.table
    set_name = args.set_name
    description = args.description
    event_set_id =  args.event_set_id
    start_date = args.start_date
    end_date = args.end_date
    tag = args.tag_suffix 
    config = args.config 
    upload_file_path = args.path
    dry_run =args.dry_run
    origin = args.origin
    staging_path = args.staging_path
    config_inputs_loc = args.config_inputs_loc
    client_warehouse_path = args.client_warehouse_path
    try:
        if staging_path is None or len(staging_path) == None:
            staging_path = "/tmp/{}_{}_{}".format( tp.lower(), tpre_started_on, origin)
        report_destpath = "{}/{}_staging_{}/".format(staging_path, tp.lower(), str(tpre_started_on))

        logging.info("\n\n access_token: {}\n account_id: {}\n  pixel_id: {}\n business_id: {} \
            \n table: {}\n set_name: {}\n description: {}\n event_set_id: {}\n start_date: {}\n end_date: {} \
            \n tag: {}\n config: {}\n upload_file_path: {}\n dry_run: {}\n origin: {} \
            \n report_destpath: {}\n config_inputs_loc: {}\n client_warehouse_path: {} \n\n\
            ".format(access_token, account_id, pixel_id, business_id, table, set_name, description, 
            event_set_id, start_date, end_date, tag, config, upload_file_path, dry_run, origin,
            report_destpath, config_inputs_loc, client_warehouse_path))


        logging.info("\n==========Facebook Remote execution has started!!==================\n")

        '''
            Facebook Remote Execution call
            
        '''
        facebook_remote_execution(access_token, account_id, pixel_id, business_id, table, set_name, description, event_set_id, 
                                  start_date, end_date, tag, config, upload_file_path, report_destpath, config_inputs_loc, 
                                  client_warehouse_path, origin, tpre_started_on, dry_run)
   
        logging.info("\n==========Facebook Remote execution has completed!!==================\n")


        if log_file is not None:
            logging.info("Find Facebook Remote Execution logs as mantioned below: \n-> {}\n".format(log_file))
        started_on = datetime.fromtimestamp(tpre_started_on)
        completed_on = datetime.fromtimestamp(int(time.time()))
        logging.info("Facebook Remote Execution Started at: {}".format(started_on))
        logging.info("Facebook Remote Execution Completed at: {}".format(completed_on))
        logging.info("Time taken by Facebook Remote Execution: {}  [#days days, hh:mm:ss]".format(str(completed_on-started_on)))
    
    except Exception as e:
        logging.info("\n==========Facebook Remote Execution has failed!!=================\n")
        if log_file is not None:
            logging.info("Find Facebook Remote Execution logs as mantioned below: \n-> {}\n".format(log_file))
        started_on = datetime.fromtimestamp(tpre_started_on)
        completed_on = datetime.fromtimestamp(int(time.time()))
        logging.info("Time taken by Facebook Remote Execution: {}  [#days days, hh:mm:ss]".format(str(completed_on-started_on)))
        traceback.print_exc(file=sys.stdout)
        logging.error(e, exc_info=True)
        traceback.print_stack()
        sys.exit(1)
    finally:
        days = 7
        logging.info('Cleaning up {} days old created/modified stuff from staging dir: {}'.format(days, staging_path))
        cleanup(days, staging_path, dry_run=False)
        logging.info('Cleaned up successfully! from staging dir: {}'.format(staging_path))
        


# if __name__ == '__main__':
#     configure_logging(loglevel=logging.INFO, is_file_handler=False)
#     main()
