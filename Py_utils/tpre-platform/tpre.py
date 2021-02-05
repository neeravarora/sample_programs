import py_path
import sys, os
import argparse
import logging
import time
import json
from datetime import datetime
from fb import fbre



def create_parser():
    parser = argparse.ArgumentParser(
        description='Uploads path records to FB using the Offline Conversions API'
    )
    parser.add_argument('--tp',
                        dest='tp',
                        action='store',
                        required=True,
                        choices=['FB', 'PINTEREST'],
                        type=str,
                        help='Third Party Selection ')
    parser.add_argument('--tpre-staging-path',
                        dest='staging_path',
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
    tpre_started_on = int(time.time())
    parser = create_parser()
    args = parser.parse_known_args()[0]
    
    staging_path = args.staging_path
    
    if staging_path is None or len(staging_path) == None:
        staging_path = "/tmp/{}_{}".format(args.tp.lower(), tpre_started_on)
    staging_path = "{}/{}_staging_{}/".format(staging_path, args.tp.lower(), str(tpre_started_on))
    log_path = os.path.join(staging_path, 'logs')
    os.makedirs(log_path, exist_ok=True)  
    
    '''
        Logs file location selection
        
    '''
    if args.tp.upper() == 'FB':
        log_file="{0}/{1}_fbre.log".format(log_path, str(tpre_started_on))
    elif args.tp.upper() == 'PINTEREST':
        log_file="{0}/{1}_tpre.log".format(log_path, str(tpre_started_on))
    else:
        log_file="{0}/{1}_tpre.log".format(log_path, str(tpre_started_on))
    
    
    # Setting up logging
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)

    stdout_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(filename=log_file)

    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s %(levelname)s %(thread)d: %(message)s',
                        handlers=[stdout_handler, file_handler])

    logging.info('Logger configured!!')
    logging.debug('Package lookup path:\n{}'.format(sys.path))
    logging.info('==========================================================')
    logging.info('TPRE started at {}'.format(str(datetime.fromtimestamp(tpre_started_on))))
    logging.debug('TPRE is Starting with... args:\n{}'.format(args))
    logging.info('TPRE starting for third Party: {}'.format(args.tp))
    logging.info('=========================================================')
    if args.tp.upper() == 'FB':
        fbre.main(args.tp, parser, tpre_started_on, log_file)
    elif args.tp.upper() == 'PINTEREST':
        logging.info('{} has not implemented yet!!'.format(args.tp))
        sys.exit(1)
