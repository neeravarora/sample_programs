import json
import time
import os, sys
import re
import logging
from shutil import copyfile

from tv import path_resolver
#from tv.util.s3_utils import S3
from tv.util.neucmd.neuhadoop import S3
from tv.ds_metadata import S3Location
from tv.util import date_utils



class State:

    def __init__(self, data_dict: dict = None):
        
        self.status = 'NA'
        self.started_on = -1
        self.finished_on = -1
        self.tv_start_date = ""
        self.tv_end_date = ""
        self.trigger_no_of_days = -1
        if data_dict is not None:
            if data_dict.get('status') is not None:
                self.status = data_dict.get('status')
            if data_dict.get('started_on') is not None:
                self.started_on = data_dict.get('started_on')
            if data_dict.get('finished_on') is not None:
                self.finished_on = data_dict.get('finished_on')
            if data_dict.get('tv-start-date') is not None:
                self.tv_start_date = data_dict.get('tv-start-date')
            if data_dict.get('tv-end-date') is not None:
                self.tv_end_date = data_dict.get('tv-end-date')
            if data_dict.get('trigger_no_of_days') is not None:
                self.status = data_dict.get('trigger_no_of_days')



    

class StateData:

    def __init__(self, type:str = 'original', path = None):
        self.type = type
        data = StateData.__load_json(path)

    @classmethod
    def __load_json(cls, path = None):
        if path is None:
            if self.type == 'original':
                path = os.path.join("state", 'original','state.json') 
            elif self.type == 'working':
                path = os.path.join("state", 'working','state.json') 
        resolved_path = path_resolver.resolve(path)
        with open(resolved_path, 'r') as qubole_config_file:       
            state_dict  = json.load(qubole_config_file)
        return state_dict





class StateService:

    logger = logging.getLogger(__name__)

    optimistic_lock = 'NA'
    last_completed_run = '1970-01-01'
    status = 'NA'
    parent_started_on = -1
    started_on = -1
    finished_on = -1
    tv_start_date = ""
    tv_end_date = ""
    trigger_no_of_days = 1
    desc = ""

    MAX_STATS = 50

    @classmethod
    def setup(cls, start_date, trigger_no_of_days = 1, started_on = int(time.time()), 
                MAX_STATS = 50, dry_run = False, mock_run=True):
        path = os.path.join("state", 'template','state.json')
        templete_file_path = path_resolver.resolve(path)
        path = os.path.join("state", 'original','state.json')
        original_file_path = path_resolver.resolve(path)
        path = os.path.join("state", 'working','state.json')
        working_file_path = path_resolver.resolve(path)
        copyfile(templete_file_path, original_file_path)
        copyfile(templete_file_path, working_file_path)
        cls.status = 'COMPLETED'
        cls.parent_started_on = started_on
        cls.started_on = started_on
        cls.finished_on = cls.started_on
        cls.tv_start_date = date_utils.date_operations(start_date, \
                            days=1)
        cls.tv_end_date = cls.tv_start_date
        cls.trigger_no_of_days = trigger_no_of_days
        cls.desc = "ETL Setup"
        cls.last_completed_run = cls.tv_end_date
        cls.MAX_STATS = MAX_STATS
        cls.stat_persist(cls.started_on, dry_run=dry_run, mock_run=mock_run)

    '''
        Needs to call to get and sync last COMPLETED STAT
    '''
    @classmethod
    def stat_sync(cls, dry_run = True, mock_run=True):
        if cls.status.upper() == "INPROGRESS":# or cls.is_locked():
            cls.logger.info("Already inprogress")
            assert False, 'Already inprogress....'
        if not mock_run:  
            cls.sync_original()
            cls.sync_working()
        cls.load_working()
        cls.status = "INPROGRESS"

    @classmethod
    def stat_persist(cls, started_on = int(time.time()), dry_run = True, mock_run = True):
        cls.logger.debug("Stat Persisting....")
        cls.upadate_working(started_on)
        if dry_run or mock_run:
            return
        cls.save_working()
        cls.logger.info("Current working stat saved!")


    @classmethod
    def load_working(cls):
        state_dict = cls.read()
        cls.last_completed_run = state_dict.get('last_completed_run')
        audit_list = state_dict['tv_etl_history']
        for stat in audit_list:
            if stat['status'].upper() == 'COMPLETED':
                cls.load_model(stat)
                break
        

    @classmethod
    def upadate_working(cls, started_on):
        cls.logger.debug("Updating working stat.....")
        state_dict = cls.read()
        if cls.status.upper() == 'COMPLETED':
            state_dict['last_completed_run'] = cls.tv_end_date
        audit_list = state_dict['tv_etl_history']
        if audit_list[0]['started_on'] == started_on:
            cls.get_state_model_dict(audit_list[0])
        else:
            audit_list.insert(0, cls.get_state_model_dict())
        if(len(audit_list) > cls.MAX_STATS):
            audit_list.pop()
        cls.write(state_dict)
        cls.logger.debug("Updated working stat!!")


    @classmethod
    def sync_working(cls):
        path = os.path.join("state", 'original','state.json')
        original_file_path = path_resolver.resolve(path)
        path = os.path.join("state", 'working','state.json')
        working_file_path = path_resolver.resolve(path)
        copyfile(original_file_path, working_file_path)
    

    @classmethod
    def sync_original(cls):
        path = os.path.join("state", 'original','state.json')
        resolved_path = path_resolver.resolve(path)
        s3_loc = os.path.join(S3Location.tv_etl_states_loc,'state.json')
        assert re.match('^.*state/state.json$',s3_loc) is not None, 'State location should have leaf dir state/state.json'
        #res = S3.s3_ftp(s3_loc, resolved_path, 'DOWNLOAD', debug=True, print_stderr=True)
        res = S3.copy(s3_loc, resolved_path, 'DOWNLOAD', debug=True, print_stderr=True)
        assert res is not None and res[0] != '' , 'Syncing with original state has failed!'
        
    @classmethod
    def save_working(cls):
        path = os.path.join("state", 'working','state.json')
        resolved_path = path_resolver.resolve(path)
        s3_loc = S3Location.tv_etl_states_loc
        assert re.match('^.*state/$',s3_loc) is not None, 'State location should have leaf dir state/'
        #res = S3.s3_ftp(s3_loc, resolved_path, 'UPLOAD', debug=True, print_stderr=True)
        res = S3.copy(resolved_path, s3_loc, 'UPLOAD', debug=True, print_stderr=True)
        assert res is not None and res[0] != '' , 'Saving working state file failed!'


    @classmethod
    def read(cls, path = None):
        if path is None:
            path = os.path.join("state", 'working','state.json') 
        resolved_path = path_resolver.resolve(path)
        with open(resolved_path, 'r') as state_file:       
            state_dict  = json.load(state_file)
        return state_dict

    @classmethod
    def write(cls, data_dict: dict, path = None):
        if path is None:
            path = os.path.join("state", 'working','state.json') 
        resolved_path = path_resolver.resolve(path)
        with open(resolved_path, 'w') as outfile:       
            json.dump(data_dict, outfile, indent=4)

    @classmethod
    def get_state_model_dict(cls, model : dict = {}):
        model['status'] = cls.status
        model['parent_started_on'] = cls.parent_started_on
        model['started_on'] = cls.started_on
        model['finished_on'] = cls.finished_on
        model['tv-start-date'] = cls.tv_start_date
        model['tv-end-date'] = cls.tv_end_date
        model['trigger_no_of_days'] = cls.trigger_no_of_days
        model['description'] = cls.desc
        return model

    @classmethod
    def load_model(cls, stat: dict = {}):
        cls.status = stat.get('status')
        cls.parent_started_on = stat.get('parent_started_on')
        cls.started_on = stat.get('started_on')
        cls.finished_on = stat.get('finished_on')
        cls.tv_start_date = stat.get('tv-start-date')
        cls.tv_end_date = stat.get('tv-end-date')
        cls.trigger_no_of_days = stat.get('trigger_no_of_days')

    @classmethod
    def read_original(cls):
        cls.sync_original()
        path = os.path.join("state", 'original','state.json') 
        return cls.read(path)

    @classmethod
    def is_locked(cls):
        path = os.path.join("state", 'lock','lock.json')
        lock_data = cls.read(path)
        if lock_data.get('process_id') is None:
            lock_data['process_id'] = os.getpid()
            cls.write(lock_data, path)
            return False
        if lock_data.get('process_id') != os.getpid():
            cls.logger.info("ETL is locked for proccess id {}".format(os.getpid()))
        return True

    @classmethod
    def release_lock(cls):
        path = os.path.join("state", 'lock','lock.json')
        lock_data = cls.read(path)
        lock_data['process_id'] = None
        cls.write(lock_data, path)

    @classmethod
    def take_lock(cls):
        path = os.path.join("state", 'lock','lock.json')
        lock_data = cls.read(path)
        if lock_data.get('process_id') is None:
            lock_data['process_id'] = os.getpid()
            cls.write(lock_data, path)
            return True
        else:
            cls.logger.info("ETL is already locked for proccess id {}".format(lock_data['process_id']))
            return False


