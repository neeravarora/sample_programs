"""Use FB API connectors upload conversion data"""

import sys
import argparse
import datetime
import logging
import time
import json
from conv_api_conn import get_reports_urls, download_report_files


def main(args):

    # Get list of available reports
    urls = get_reports_urls(args.access_token, args.business_id, args.pixel_id)

    # Download reports
    download_report_files(urls, path=args.destpath)


def create_parser():
    parser = argparse.ArgumentParser(
        description='Uploads path records to FB using the Offline Conversions API'
    )
    parser.add_argument('--access-token',
                        dest='access_token',
                        action='store',
                        required=True,
                        type=str,
                        help='Access token for API')
    parser.add_argument('--business-id',
                        dest='business_id',
                        action='store',
                        required=True,
                        type=str,
                        help='Neustar business ID in FB')
    parser.add_argument('--pixel-id',
                        dest='pixel_id',
                        action='store',
                        required=True,
                        type=str,
                        help='Client Pixel ID in FB')
    parser.add_argument('--destination-path',
                        dest='destpath',
                        action='store',
                        required=True,
                        type=str,
                        help='Path where to save report files')
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
