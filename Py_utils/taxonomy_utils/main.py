from libs import py_path
import sys, os
import argparse
import logging
import time
import json
from datetime import datetime
import traceback

from service.taxonomy_cs_api import Taxonomy_CS_API



def create_parser():
    parser = argparse.ArgumentParser(
        description='Taxonomy CS'
    )
    parser.add_argument('--run',
                        dest='run',
                        action='store',
                        required=False,
                        choices=['TAXONOMY_CS'],
                        type=str,
                        default='TAXONOMY_CS',
                        help='Operation type')
    parser.add_argument('--src',
                        dest='lmt_src',
                        action='store',
                        required=True,
                        type=str,
                        help='Source/Input csv files s3 location path')
    parser.add_argument('--data',
                        dest='lmt_data',
                        action='store',
                        required=True,
                        type=str,
                        help='Data csv files s3 location path')
    parser.add_argument('--config',
                        dest='config',
                        action='store',
                        required=True,
                        type=str,
                        help='Path to the platform_config.xml file for the client')
    parser.add_argument('--config-loc',
                        dest='config_inputs_loc',
                        action='store',
                        required=True,
                        type=str,
                        help='Project config inputs location path')
    parser.add_argument('--config-file-name',
                        dest='config_file_name',
                        action='store',
                        required=True,
                        type=str,
                        help='Project config file name for output config')
    parser.add_argument('--dn-version',
                        dest='dn_version',
                        action='store',
                        required=True,
                        type=str,
                        help='DN Version')
    parser.add_argument('--log',
                        dest='loglevel',
                        action='store',
                        choices=['WARNING', 'INFO', 'DEBUG'],
                        required=False,
                        default='INFO',
                        type=str,
                        help='logging level')
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
    assert args.lmt_src is not None, "--src is required!!"
    assert args.lmt_data is not None, "--data is required!!"
    assert args.config_inputs_loc is not None, "--config-loc is required!!"
    assert args.config_file_name is not None, "--config-file-name is required!!"
    assert args.config is not None, "--config is required!!"
    assert args.dn_version is not None, "--dn-version is required!!"
    

    


def main(args):
    
    tcAPI = Taxonomy_CS_API(lmt_src = args.lmt_src, 
                 lmt_data = args.lmt_data, 
                 config = args.config, 
                 config_input_loc = args.config_inputs_loc, 
                 config_file_name = args.config_file_name, 
                 dn_version = args.dn_version, 
                 dry_run = args.dry_run)

    tcAPI.titled('...TAXONOMY CS RUN...')
    tcAPI.delta()
    
 


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    validate_input(args)

     # Setting up logging
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)

    stdout_handler = logging.StreamHandler(sys.stdout)
    #file_handler = logging.FileHandler(filename=log_file)

    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s %(levelname)s %(thread)d: %(message)s',
                        handlers=[stdout_handler])
    # logging.basicConfig(level=numeric_level,
    #                     format='%(asctime)s %(levelname)s %(thread)d: %(message)s',
    #                     handlers=[stdout_handler, file_handler])
    
    main(args)

    
    
