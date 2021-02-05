"""Use FB API connectors upload conversion data"""

import sys
import os
import argparse
import datetime
import logging
import time
import json
from conv_api_conn import upload_events, upload_events_path, create_event_set, attach_event_set


def is_date(s):
    """Checks if string supplied is in correct date format"""
    try:
        datetime.datetime.strptime(s, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def main(args):

    # Generate event set if not available
    if not args.event_set_id:
        event_set_id = create_event_set(args.access_token, args.business_id, args.set_name, args.description)
        attach_event_set(args.access_token, event_set_id, args.business_id, args.account_id)
    else:
        event_set_id = args.event_set_id
    logging.info("Event set ID: %s", event_set_id)
    # Upload data
    results = {}
    tag = args.tag_suffix
    logging.info("Uploading {}, {}, {}".format(tag, args.start_date, args.end_date))
    if args.table is not None:
        results[tag] = upload_events(
            args.table, args.access_token,
            event_set_id, args.start_date, args.end_date,
            tag, args.config, args.dry_run)
    else:
        if not os.path.isdir(args.path):
            raise Exception("Folder not found: " + args.path)
        results[tag] = upload_events_path(
            args.path, args.access_token,
            event_set_id, args.start_date, args.end_date,
            tag, args.config, args.dry_run)

    # Writing results to a file
    with open('conversions_upload_output_{:.0f}.json'.format(time.time()), 'w') as outfile:
        json.dump(results, outfile, indent=2)


def create_parser():
    parser = argparse.ArgumentParser(
        description='Uploads path records to FB using the Offline Conversions API'
    )
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
                        required=False,
                        type=str,
                        help='Required if an event-set-id is not provided')
    parser.add_argument('--event-set-name',
                        dest='set_name',
                        action='store',
                        required=False,
                        type=str,
                        default=None,
                        help='Free from event set name. Required if an event-set-id is not provided')
    parser.add_argument('--event-set-description',
                        dest='description',
                        action='store',
                        required=False,
                        type=str,
                        default=None,
                        help='Free-form event set description. Required if an event-set-id is not provided')
    parser.add_argument('--account-id',
                        dest='account_id',
                        action='store',
                        required=False,
                        type=str,
                        default=None,
                        help='Required if an event-set-id is not provided')
    parser.add_argument('--dry-run',
                        dest='dry_run',
                        action='store_true',
                        help='If passed, no API calls are made. Requires an event-set-id to be provided')
    parser.add_argument('--log',
                        dest='loglevel',
                        action='store',
                        choices=['WARNING', 'INFO', 'DEBUG'],
                        required=False,
                        default='INFO',
                        type=str,
                        help='logging level')
    return parser


if __name__ == '__main__':

    parser = create_parser()
    args = parser.parse_args()

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

    # Setting up logging
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)

    stdout_handler = logging.StreamHandler(sys.stdout)

    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s %(levelname)s %(thread)d: %(message)s',
                        handlers=[stdout_handler])

    logging.info('Starting... args:\n{}'.format(args))
    logging.debug('Package lookup path:\n{}'.format(sys.path))

    main(args)
