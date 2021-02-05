from tv import py_path
from tv import path_resolver
import sys, os
import argparse
import logging
import time
import json
from datetime import datetime
import traceback
from tv.services.ds_sync import DBMetadataSync
from tv.etl import TVETL
from tv.configs import Config
from tv.filelock.filelock import FileLock
from tv.audit import Audit


def create_parser():
    parser = argparse.ArgumentParser(
        description='TV ETL trigger'
    )
    parser.add_argument('--tv-etl',
                        dest='tv_etl',
                        action='store',
                        required=True,
                        choices=['SETUP', 'DEFAULT_TRIGGER', 'MANUAL_TRIGGER', 'DS_SYNC', 'STAT'],
                        type=str,
                        help='Operation type')
    parser.add_argument('--start-date',
                        dest='start_date',
                        action='store',
                        required=False,
                        type=str,
                        help='Transformation start date')
    parser.add_argument('--end-date',
                        dest='end_date',
                        action='store',
                        required=False,
                        type=str,
                        help='Transformation end date')
    parser.add_argument('--output',
                        dest='output',
                        action='store',
                        required=False,
                        type=str,
                        help='Result S3 location')
    parser.add_argument('--ultimate-owner-id-list',
                        dest='ultimate_owner_id_list',
                        metavar='dim',
                        nargs='+',
                        action='store',
                        required=False,
                        type=str,
                        help='Ultimate owner id list to generate impressions')
    parser.add_argument('--filter',
                        dest='filter',
                        choices=['LIMITED_ULTIMATEOWNER_ID', 'ALL_ULTIMATEOWNER_ID'],
                        default='LIMITED_ULTIMATEOWNER_ID',
                        action='store',
                        required=False,
                        type=str,
                        help='Working dir')
    parser.add_argument('--tv-dir',
                        dest='tv_dir',
                        action='store',
                        required=False,
                        type=str,
                        help='Working dir')
    parser.add_argument('--type',
                        dest='type',
                        action='store',
                        required=False,
                        choices=['LOCAL', 'SAVED'],
                        default='LOCAL',
                        type=str,
                        help='Stat Type')
    parser.add_argument('--max-stat',
                        dest='max_stat',
                        action='store',
                        required=False,
                        default=5,
                        type=int,
                        help='Maximum number of stat record')
    parser.add_argument('--status',
                        dest='status',
                        action='store',
                        required=False,
                        default='',
                        type=str,
                        help='Stat status')
    parser.add_argument('--log',
                        dest='loglevel',
                        action='store',
                        choices=['WARNING', 'INFO', 'DEBUG'],
                        required=False,
                        default='INFO',
                        type=str,
                        help='logging level')
    parser.add_argument('--schema-change',
                        dest='schema_change',
                        action='store',
                        choices=[True, False],
                        required=False,
                        default=False,
                        type=bool,
                        help='If passed, no Schema change API calls are made.')
    parser.add_argument('--dry-run',
                        dest='dry_run',
                        action='store',
                        choices=[True, False],
                        required=False,
                        default=False,
                        type=bool,
                        help='If passed, no Write API calls are made.')
    parser.add_argument('--mock-run',
                        dest='mock_run',
                        action='store',
                        choices=[True, False],
                        required=False,
                        default=False,
                        type=bool,
                        help='If passed, no API calls are made.')

    return parser

def validate_input(args):

    # Validating inputs
    assert args.tv_etl is not None, "--TV-ETL is a Manadatory"
    if args.tv_etl.upper() == 'SETUP':
        assert args.start_date is not None, "--start-date is required!! "
    elif args.tv_etl.upper() == 'MANUAL_TRIGGER':
        assert args.start_date is not None, "--start-date is required!! "
        assert args.end_date is not None, "--end-date is required!! "
        if args.ultimate_owner_id_list is not None:
            assert len(args.ultimate_owner_id_list) > 0, "ultimate_owner_id_list should not be empty"

    # if args.tv_etl.upper() == 'DEFAULT_TRIGGER':
    #     assert args.start_date is not None, "--start-date is required!! "
    #     assert args.end_date is not None, "--end-date is required!! "
    #     assert is_date(args.start_date), "start_date is not in the correct date format (yyyy-mm-dd)"
    #     assert is_date(args.end_date), "end_date is not in the correct date format (yyyy-mm-dd)"

def is_date(date_str):
    """Checks if string supplied is in correct date format"""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def get_staging_dir(tv_dir, tv_started_on, cnt=1):
    
    if tv_dir is not None and len(tv_dir) > 0:
        if os.path.exists(tv_dir) and os.path.isdir(tv_dir):
            #tv_dir = "{}/tv-etl/{}_{}/".format(tv_dir, 'tv-etl', str(tv_started_on))
            tv_dir = os.path.join(tv_dir, 'tv-etl', '{}_{}'.format('tv-etl', str(tv_started_on)))
            os.makedirs(tv_dir, exist_ok=True)
        else:
            logging.warning('ERRORED: Staging_dir:'+ tv_dir)
            if cnt == 1:
                logging.warning('ERRORED: Given Staging_dir path as part --tv-dir is not a valid path\n\n\n')
            elif cnt==2:
                logging.warning('ERRORED: Staging_dir path as part of staging_dir_path in config.properties is not valid path\n\n\n')
            assert False, "\n      tv_dir:  {} doesn't have a valid path \
            \n      Please make sure path should be valid with Read/Write permissions".format(str(tv_dir))
    else:
        tv_dir = Config.staging_dir(tv_dir)
    if cnt < 2:
        return get_staging_dir(tv_dir, tv_started_on, cnt+1)
    
    if tv_dir is None or len(tv_dir) == 0:
        tv_dir = "/tmp/tv-etl/{}_{}".format('tv-etl', str(tv_started_on))
        Config.staging_dir(tv_dir)
    print('\n Staging_dir:  {}\n'.format(tv_dir))
    
    return tv_dir


def main(args):
    
    logging.info('=================================================================')
    logging.info('TV Pre-processing System started at {}'.format(str(datetime.fromtimestamp(tv_started_on))))
    logging.debug('TV Pre-processing System is Starting with... args:\n{}'.format(args))
    logging.info('TV Pre-processing System starting for operation: {}'.format(args.tv_etl))
    logging.info('TV Pre-processing SQL Execution Engine: {}'.format(Config.execution_engine))
    logging.info('=================================================================\n\n')
    
    # logging.debug('Package lookup path:\n{}'.format(sys.path))
    logging.debug('--tv-etl :' +str(args.tv_etl))
    logging.debug('--start-date :' +str(args.start_date))
    logging.debug('--end-date :' +str(args.end_date))
    logging.debug('--output :' +str(args.output))
    logging.debug('--tv-dir :' +str(args.tv_dir))
    logging.debug('--log :' +str(args.loglevel))
    logging.debug('--Schema-change :' +str(args.schema_change))
    logging.debug('--dry-run :' +str(args.dry_run))
    logging.debug('--mock-run :' +str(args.mock_run))
    logging.info('=================================================================\n\n')

    tvetl: TVETL = TVETL(tv_started_on)
    if args.tv_etl.upper() == 'SETUP':
        logging.info('TV ETL is getting setup!!')
        tvetl.set_up(args.start_date, started_on=tv_started_on, schema_change = args.schema_change,
                     dry_run = args.dry_run, mock_run = args.mock_run)
        logging.info('TV ETL Setup is completed!!')
    elif args.tv_etl.upper() == 'DS_SYNC':
        db_metadataSync : DBMetadataSync = DBMetadataSync()
        db_metadataSync.sync(dry_run=args.dry_run, mock_run=args.mock_run)
        db_metadataSync.output_sync(dry_run=args.dry_run, mock_run=args.mock_run)
    elif args.tv_etl.upper() == 'DEFAULT_TRIGGER':
        logging.info('TV ETL DEFAULT transforming has been triggered!!')
        # tvetl.default_etl(args.trigger_no_of_days, started_on=tv_started_on, dry_run = args.dry_run, mock_run = args.mock_run)
        tvetl.default_etl(started_on=tv_started_on, dry_run = args.dry_run, mock_run = args.mock_run)
        
        started_on_date = datetime.fromtimestamp(tv_started_on)
        completed_on_date = datetime.fromtimestamp(int(time.time()))
        logging.info("The Complete Time taken by TV ETL Default Transformation Command: {}  \
                    [#days days, hh:mm:ss]".format(str(completed_on_date-started_on_date)))
        logging.info('TV ETL DEFAULT transforming has been completed!!')
    elif args.tv_etl.upper() == 'MANUAL_TRIGGER':
        logging.info('TV ETL MANUAL transforming has been triggered!!')
        ultimate_owner_id_list = args.ultimate_owner_id_list
        logging.info('TV ETL MANUAL transforming triggered for ultimate owner ids :'+str(ultimate_owner_id_list)) 
        started_on_date = datetime.fromtimestamp(tv_started_on)
        completed_on_date = datetime.fromtimestamp(int(time.time()))
        tvetl.manual_etl(args.start_date, args.end_date, result_loc=args.output, ultimate_owner_id_list=ultimate_owner_id_list, 
        filter=args.filter, started_on=tv_started_on, dry_run = args.dry_run, mock_run = args.mock_run)
        logging.info("The Complete Time taken by TV ETL MANNUAL Transformation Command: {}  \
                    [#days days, hh:mm:ss]".format(str(completed_on_date-started_on_date)))

    else:
        assert False, 'Invalid Trigger'


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    validate_input(args)
    Config.intialise()
    if args.tv_etl.upper() == 'STAT':
        Audit.stat(type=args.type, max_stat=args.max_stat, status=args.status)
        sys.exit(0)
    
    tv_started_on = int(time.time())
    tv_dir = args.tv_dir
    tv_dir = get_staging_dir(tv_dir, tv_started_on)
    Config.staging_loc_path(tv_dir)

    log_path = os.path.join(tv_dir, 'logs')
    os.makedirs(log_path, exist_ok=True) 
    log_file="{0}/{1}_tv.log".format(log_path, str(tv_started_on))

    # Setting up logging
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)

    stdout_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(filename=log_file)

    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s %(levelname)s %(thread)d: %(message)s',
                        handlers=[stdout_handler, file_handler])
#     logging.basicConfig(level=numeric_level,
#                         format='%(asctime)s %(levelname)s %(name)s: %(message)s',
#                         handlers=[stdout_handler])

    logging.debug('Logger configured!!\n\n')
    logging.debug('\n\nPROJECT_ROOT_PATH:  {} \n\n'.format(path_resolver.get_project_root()))

    path = os.path.join("state", 'lock')
    lock_path = path_resolver.resolve(path)
    etl_lock_path = os.path.join(lock_path, 'etl')
    logging.debug("Going to Locked with file descriptor: \n\n  {}.lock\n".format(str(etl_lock_path)))
    try:
        with FileLock(etl_lock_path):
            try:
                logging.info('TV ETL Occupied Lock!!')
                time.sleep(10)
                main(args)
                
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
                logging.error(e, exc_info=True)
                traceback.print_stack()

        logging.info('TV ETL Released Lock!!')
    except Exception as e:
        logging.info('===============================================')
        logging.info('      TV ETL is Locked!! Already inprogess')
        logging.info('===============================================\n\n\n')
        #traceback.print_exc(file=sys.stdout)
        #logging.error(e, exc_info=True)
        #traceback.print_stack()
    
    
