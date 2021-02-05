import os, sys
import time
import subprocess
import shlex
import json
import re
import gzip
import getpass
import threading
import logging
import glob
from queue import Queue
from threading import current_thread
from multiprocessing import current_process
import pandas as pd
import unittest
import traceback
import io
import configparser
from lxml import etree
from tv import path_resolver
from tv.ds_metadata import System_Prop, TableName, S3Location

from env import ENV_CNF_DIR


class Config:

    json_config_dict = None
    configs_data = None
    staging_dir_path = None
    staging_loc = None
    platform_config_loc = None
    execution_engine = 'SPARK' #HIVE or SPARK

    logger = logging.getLogger(__name__)

    @classmethod
    def intialise(cls):
        config_data = cls.get_configs()
        System_Prop.init(config_data.get('profile'), config_data.get('db_suffix'))
        S3Location.set_configs(config_data)
        cls.execution_engine = config_data.get('execution_engine')
        

    '''
        qubole_config from json
    '''
    @classmethod
    def get_qubole_config_json(cls, path = None):
        if path is None:
            path = os.path.join("conf", 'qubole_config.json') 
        resolved_path = path_resolver.resolve(path)
        with open(resolved_path, 'r') as qubole_config_file:       
            config_dict  = json.load(qubole_config_file)
        return config_dict

    @classmethod
    def create_qubole_config_dict(cls, cluster, s3_location, access_key, secret_key, token, url):
        result : dict = dict()
        result['cluster'] = cluster
        result['s3_location'] = s3_location
        result['access_key'] = access_key
        result['secret_key'] = secret_key
        result['token'] = token
        result['url'] = url
        return result

    '''
        qubole_config from platform config xml
    '''
    @classmethod
    def get_qubole_config(cls, config):
            tree = etree.parse(config)
            platform = tree.xpath("/configroot/set[name='PLATFORM']/stringval")[0].text.lower()
            if platform == "qubole":
                fields = ['s3_location', 'token', 'url', 'cluster',
                        'access_key', 'secret_key']
            else:
                return "errored Qubole should be enable!!"
            result = {}
            for field in fields:
                result[field] = tree.xpath(
                    "//{}_settings/set[name='{}']/stringval".format(platform, field)
                )[0].text.strip()
            return result

    @classmethod
    def get_json_configs(cls):
        if cls.json_config_dict is None:
            cls.json_config_dict = cls.get_qubole_config_json()
        return cls.json_config_dict

    @classmethod
    def get_platform_config(cls, platform_config_loc=None):
        if platform_config_loc is not None:
            cls.platform_config_loc = platform_config_loc
            return cls.platform_config_loc

        if cls.platform_config_loc is not None:
            return cls.platform_config_loc

        home_dir = os.path.expanduser("~")
        platform_config_loc = None
        if home_dir is not None:
            platform_config_dir = os.path.join(home_dir, '.tv_etl')
            platform_config_loc = cls.__search_cfg_file(platform_config_dir)

        if platform_config_loc is None:
            platform_config_dir = path_resolver.resolve("conf")
            platform_config_loc = cls.__search_cfg_file(platform_config_dir)

        if platform_config_loc is not None:
            cls.platform_config_loc = str(platform_config_loc)
        return cls.platform_config_loc


    @classmethod
    def staging_dir(cls, staging_loc= None):
        if staging_loc is None or len(staging_loc) == 0:
            if cls.configs_data is None:
                cls.intialise()
            cls.staging_dir_path = cls.configs_data.get('staging_dir_path')
            #if cls.staging_loc is None:
            return cls.staging_dir_path
        if cls.staging_loc is None and staging_loc is not None:
            cls.staging_loc = str(staging_loc)
            
        return cls.staging_loc

    @classmethod
    def staging_loc_path(cls, staging_loc= None):
        if staging_loc is not None:
            cls.staging_loc = str(staging_loc)
        return cls.staging_loc


    @classmethod
    def get_configs(cls):

        if cls.configs_data is not None:
            return cls.configs_data
        
        config = configparser.RawConfigParser()
        conf_file_patrn = "config.properties"

        home_dir = os.path.expanduser("~")
        print("User Home Dir: "+os.path.expanduser("~"))
        cls.logger.debug("User Home Dir: "+os.path.expanduser("~"))
        config_loc = None
        if home_dir is not None:
            config_dir = os.path.join(home_dir, ENV_CNF_DIR)
            config_loc = cls.__search_file(config_dir, conf_file_patrn)
            
        if config_loc is None:
            config_dir = path_resolver.resolve("conf")
            config_loc = cls.__search_file(config_dir, conf_file_patrn)

        if config_loc is None:
            assert False, "config.properties file is missing."
        
        cls.logger.debug("config_loc: "+config_loc)
        config.read(config_loc)
        if config.defaults() is  None or config.defaults().get('profile') is None:
            assert False, "Default Section with profile is missing in config: {}".format(config_loc)
            
        profile = config.defaults().get('profile')
        if profile is None or len(profile.strip()) == 0:
            assert False, "profile is missing in default section of config {}".format(profile)
        
        db_suffix = config.defaults().get('db_suffix')
        if db_suffix is not None and len(db_suffix.strip()) > 0:
            section = profile + db_suffix
            
            if config.has_section(section):
                cls.logger.warn("\n Config:- Configuration picked from section:[{}] of \
                \n        config: {}\n".format(section, config_loc))
                
                cls.configs_data: dict = dict(config[section])      
                return cls.configs_data
        
            cls.logger.warn("\n Warning:- Section: [{}] is not exist. \
                \n           Therefore checking for section: [{}] in \
                \n           config: {}\n".format(section, profile, config_loc))

        if not config.has_section(profile):
            assert False, "Section {} is missing in {} ".format(profile, config_loc)

        cls.configs_data: dict = dict(config[profile])      

        return cls.configs_data
    

    @classmethod
    def __search_cfg_file(cls, cfg_dir):
        platform_cfg_patn = '*platform_config.xml'
        return cls.__search_file(cfg_dir, platform_cfg_patn)

    @classmethod
    def __search_file(cls, dir, file_patern):
        search_path = os.path.join(dir, file_patern)
        configs = glob.glob(search_path)
        if configs is not None and len(configs) > 0 :
            if len(configs) == 1:
                return configs[0]
            else:
                assert False, "More than 1 platform configs exists in {}.".format(dir)
        return None




   
