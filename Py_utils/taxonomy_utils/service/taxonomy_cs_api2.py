import sys, os, inspect, re
import time, logging
from collections import defaultdict

from libs.s3_ops import S3_OPs
from libs.s3_stream import S3Stream
from libs.configs import Config
from libs.nio_executor import NIO
from libs import utils


TG_EXTRACT_REGEX = '^.*?/([a-zA-Z]+\-?[0-9]*)/$'
FILE_EXTRACT_REGEX = '^.*/([a-zA-Z0-9.\-_]{0,255}.csv)$'  #'^.*?/([a-zA-Z]+\-?[0-9]{0-255}.csv)$'

KEY_REGEX = '^[Kk]ey_[A-Za-z0-9_]{3,30}$'
TARGET_REGEX = '^[Tt]arget_[A-Za-z0-9_]{3,30}$'

TARGET_EXTRACT_REGEX ='^.*,?(target_[A-Za-z0-9_-]+).*$'

VALID_FILE_KEY_REGEX = '^(.*/([a-zA-Z]+\-?[0-9]*)?/)?(([a-zA-Z]+\-?[0-9]*?)_([0-9]{4}-[0-9]{2}-[0-9]{2}?)_([a-zA-Z0-9.\-_]+?).csv?)$'

# VALID_FILE_KEY_REGEX = '^(.*/([a-zA-Z]+\-?[0-9]*)?/)?(([a-zA-Z]+\-?[0-9]*?)_([0-9]{4}-[0-9]{2}-[0-9]{2}?)_([a-zA-Z0-9.\-_]+?).csv?)$'


# config = '/home/vbhargava/feature_test0/msaction_backend/customers/raj_ford_test/common/config/inputs/platform_config.xml'
# config_data = Config.get_qubole_config(config=config)
# s3_ops = S3_OPs(config_data['access_key'], config_data['secret_key'])
# s3_stream = S3Stream(config_data['access_key'], config_data['secret_key'])

class Taxonomy_CS_API:
    

    
    
    def __init__(self, 
                 lmt_src = 's3://qubole-ford/taxonomy_cs/lmt/input/', 
                 lmt_data = 's3://qubole-ford/taxonomy_cs/lmt/data/', 
                 config = '/home/vbhargava/feature_test0/msaction_backend/customers/raj_ford_test/common/config/inputs/platform_config.xml'):
        
        config_data = Config.get_qubole_config(config)
        
        self.ACCESS_KEY_DATA=config_data['access_key']
        self.SECRET_KEY_DATA=config_data['secret_key']
        self.ACCESS_KEY_SRC=config_data['access_key']
        self.SECRET_KEY_SRC=config_data['secret_key']
        
        self.lmt_src = lmt_src
        self.lmt_data = lmt_data
        
        self.s3_ops_src = S3_OPs(self.ACCESS_KEY_SRC, self.SECRET_KEY_SRC)
        self.s3_ops_data = S3_OPs(self.ACCESS_KEY_DATA, self.SECRET_KEY_DATA)
        
        self.s3_stream_src = S3Stream(self.ACCESS_KEY_SRC, self.SECRET_KEY_SRC)
        self.s3_stream_data = S3Stream(self.ACCESS_KEY_DATA, self.SECRET_KEY_DATA)
        

    
    def is_valid_file(self, key:str='', regex = VALID_FILE_KEY_REGEX):
        if re.match(regex, key) is None:
            return False
        return True

    def extract_info(self, key:str='', regex = VALID_FILE_KEY_REGEX):
        matched = re.findall(regex, key)
        return {
                'KeyDirPath' : matched[0][0],
                'ParentDir' : matched[0][1],
                'FileName' : matched[0][2],
                'FileGrp' :  matched[0][3],
                'Date' :  matched[0][4],
                'ClientName' : matched[0][5]
               }
    
    def extract_info_with_bucket(self, key:str='', bucket = ''):
        res = self.extract_info(key)
        res.update({'Bucket' : bucket})
        return res
    
    
    
    def filename_by_key(self, key):
        return self.get_val_by_regex(key, FILE_EXTRACT_REGEX, error_msg="Not vaild key for taxonomy data csv file")

    def tg_by_prefix(self, key):
        return self.get_val_by_regex(key, TG_EXTRACT_REGEX, error_msg="Not vaild taxonomy data dir")


    def get_val_by_regex(self, key, regex, error_msg="can't be extract a val."):
        matched = re.findall(regex, key)
        if len(matched) > 0:
            return matched[0]
        else:
            raise Exception(error_msg)
        
    def get_data_n_schema_task(self, tg, data_files_loc):
        s3_ops = self.s3_ops_data
        s3_stream = self.s3_stream_data
        
        data_file_lock_detail = s3_ops.get_bucket_name(data_files_loc)
        files = s3_ops.list_complete(data_file_lock_detail['bucket'], data_file_lock_detail['key'])
        res = {}
        if len(files)>0:
#             s3_stream = S3Stream(ACCESS_KEY, SECRET_KEY)
            schema = s3_stream.get_header(s3_ops.get_full_s3_path(data_file_lock_detail['bucket'],files[0]['Key']))
            #res[tg]={'schema':schema, 'files': files}
            res['schema'] = {tg:schema}
            res['files'] = {tg:files}
        return res
    
    def extract_schema(self, schema):
        return schema.replace(" ","").lower()

    def validate_schema(self, schema):

        if schema=='': 
            return {'IsValid' : False, 'schema': schema, 'message' : "Schema shouldn't be empty"}
        tokens = schema.split(',')
        if len(tokens) < 2:
             return {'IsValid' : False, 'schema': schema, 'message' : "Schema should have at least 2 columns"}
        KEY_REGEX = '^[Kk]ey_[A-Za-z0-9_]{3,30}$'
        TARGET_REGEX = '^[Tt]arget_[A-Za-z0-9_]{3,30}$'
        key_cnt = 0
        target_cnt = 0
        invalid_headers = []
        columns = defaultdict(list)
        res = {}
        target_col = None
        key_cols_set = set()
        for t in tokens:
            t = t.strip()
            if re.match(TARGET_REGEX, t):
                target_cnt = target_cnt + 1
                target_col = t
            elif re.match(KEY_REGEX, t):
                key_cnt = key_cnt + 1
                key_cols_set.add(t)
            else:
                invalid_headers.append(t)
            columns[t.lower()].append(1)

        error_msgs=[]
        if target_cnt != 1 :
            error_msgs.append("Exact one Target column is required!")
        if key_cnt < 1 :
            error_msgs.append("At least one Key column is required!")
        if len(invalid_headers) > 0 :
            error_msgs.append("All given columns should Key or Target!")
        for k, v in columns.items():

            if len(v) > 1:
                print("--")
                error_msgs.append("Same name: {} should not represent more than one column in schema! cols names are case insensitive. ".format(k))

        if len(error_msgs) > 0:
            return {'IsValid' : False, 'schema': schema, 'errors' : " \n".join(error_msgs)}
        #print(str(key_cnt)+":"+str(target_cnt)+":"+str(invalid_headers)+":"+str(columns))
        return {'IsValid' : True, 'Schema': schema.replace(" ","").lower(), 
                'TargetCol' : target_col, 'KeyColsSet' : key_cols_set}


    def extract_data_detail(self, max_workers=25):
        '''
        Valid data Taxonomy Grps
    
        '''
        s3_ops = self.s3_ops_data
        
        lmt_data_loc_detail = s3_ops.get_bucket_name(self.lmt_data)
        lmt_data_loc_bucket = lmt_data_loc_detail['bucket']
        lmt_data_loc_key = lmt_data_loc_detail['key']
        valid_tg_list_res = s3_ops.list_subdirs(lmt_data_loc_bucket,lmt_data_loc_key,)

        valid_tgrp_loc_list = [[self.tg_by_prefix(item['Prefix']), 
                                '{}{}'.format(self.lmt_data, self.tg_by_prefix(item['Prefix']))] 
                               for item in valid_tg_list_res]

        collected = NIO.decorated_run_io(task=self.get_data_n_schema_task, task_n_args_list=valid_tgrp_loc_list, 
                                         max_workers=max_workers,)

        tg_data_schema_dict = {k:self.extract_schema(v)  for item in collected for k, v in item['result']['schema'].items()}
#         tg_data_files_dict = {k:{u['Key']:u for u in v } for item in collected for k, v in item['result']['files'].items()}
        tg_data_files_dict = {k:{self.filename_by_key(u['Key']):u for u in v } 
                              for item in collected for k, v in item['result']['files'].items()}
    
        target_data_tg_dict = {re.findall(TARGET_EXTRACT_REGEX,V)[0]: K for K, V in tg_data_schema_dict.items()}
    
        return {'tg_data_schema_dict': tg_data_schema_dict, 
                'tg_data_files_dict' :tg_data_files_dict, 
                'target_data_tg_dict': target_data_tg_dict}
    
    
    
 ########################################


    def grouped_tg(self, collected, tg_files_dict_type='new_tg_files_dict'):
        collect = defaultdict(dict)
        tg_f_gen = (item['result'][tg_files_dict_type] for item in collected if len(item['result'][tg_files_dict_type]) > 0)
        tg_f_gen2 = (collect[tg].update({filename: file_dict})  for item in tg_f_gen 
                     for tg, file_detail_dict in item.items() for filename, file_dict in file_detail_dict.items())
        [ i for i in tg_f_gen2]
        tg = dict(collect)
        return tg

    def grouped_flag_dict(self, collected, flag_dict_type='schema_tg_dict'):
        f_gen = (item['result'][flag_dict_type] for item in collected if len(item['result'][flag_dict_type]) > 0)
        collect = defaultdict(set)
        f_gen2 = (collect[K].add(V)  for item in f_gen for K, V in item.items())
        [ i for i in f_gen2]
        res = dict(collect)
        return res

    def grouped_set_of_flags_dict(self, collected, flag_dict_type='schema_tg_dict'):
        f_gen = (item['result'][flag_dict_type] for item in collected if len(item['result'][flag_dict_type]) > 0)
        collect = defaultdict(set)
        f_gen2 = (collect[K].add(*V)  for item in f_gen for K, V in item.items())
        [ i for i in f_gen2]
        res = dict(collect)
        return res

    def grouped_set_of_flags(self, collected, flag_dict_type='invalid_schema_files'):
        res_set=set()
        f_gen = (res_set.update(item['result'][flag_dict_type]) 
                 for item in collected if len(item['result'][flag_dict_type]) > 0)
        [ i for i in f_gen]
        return res_set

    
    def file_process_task(self, src_file_details):
        
        s3_ops = self.s3_ops_src
#         s3_ops = S3_OPs(self.ACCESS_KEY_SRC, self.SECRET_KEY_SRC)
        tg_data_schema_dict = self.tg_data_schema_dict
        tg_data_files_dict = self.tg_data_files_dict
#         tg_data_schema_dict = {}
#         tg_data_files_dict = {}

        s3_stream=self.s3_stream_src
#         s3_stream = S3Stream(self.ACCESS_KEY_SRC, self.SECRET_KEY_SRC)
        
        invalid_schema_files = set()

        ''' {'key_evt_advertiser_key,target_evt_advertiser_name': {'tg1', 'tg2', ...}}'''
        schema_tg_dict = {}

        ''' {'target_evt_advertiser_name': {'tg1', 'tg2', ...}}'''
        target_tg_dict = {}

        ''' {'tg': {'key_evt_advertiser_key,target_evt_advertiser_name', '',...}}'''
        new_tg_schema_dict = {}
        ''' {'tg': {'AdvertiserReporting_2020-06-01_ford.csv': {file detailed obj dict} }  }'''
        new_tg_files_dict = {}
        ''' {'tg': {'AdvertiserReporting_2020-06-01_ford.csv': {file detailed obj dict} }  }'''
        existing_tg_files_dict = {}


    #     src_file_details = valid_file_arg[0]
        src_file_loc = s3_ops.get_full_s3_path(src_file_details['Bucket'], src_file_details['Key'])

        schema =  s3_stream.get_header(src_file_loc)
        #schema = 'key_evt_advertiser_key, targe_evt_advertiser_name'
        validate_res = self.validate_schema(schema)
        if validate_res['IsValid']:
            src_file_details['Schema'] = validate_res['Schema']
            tg = src_file_details['FileGrp']
            file_name = src_file_details['FileName']
            schema_tg_dict[validate_res['Schema']] = tg
            target_tg_dict[validate_res['TargetCol']] = tg
            if tg_data_schema_dict.get(tg) is None:
                new_tg_schema_dict[tg] = validate_res['Schema']
                new_tg_files_dict[tg] = {file_name: src_file_details}
            else:
                existing_tg_files_dict[tg] = {file_name: src_file_details}

        else:
            invalid_schema_files.add((src_file_loc, schema, validate_res['errors']))

        return {'invalid_schema_files': invalid_schema_files,
                'schema_tg_dict': schema_tg_dict,
                'target_tg_dict':target_tg_dict,
                'new_tg_schema_dict': new_tg_schema_dict,
                'new_tg_files_dict' : new_tg_files_dict,
                'existing_tg_files_dict' : existing_tg_files_dict
               }
    
    
    def src_list_page_process_task(self, list_page, max_workers=25):
        
        log = logging.getLogger('src_list_page_process_task')
        s3_ops = self.s3_ops_src
#         s3_ops = S3_OPs(self.ACCESS_KEY_SRC, self.SECRET_KEY_SRC) 
        lmt_src = self.lmt_src
    
        lmt_src_loc_detail = s3_ops.get_bucket_name(lmt_src)
        lmt_src_loc_bucket = lmt_src_loc_detail['bucket']
        lmt_src_loc_key = lmt_src_loc_detail['key']

        invalid_files_set = { s3_ops.get_full_s3_path(lmt_src_loc_bucket, item['Key']) 
                             for item in list_page if  not self.is_valid_file(key=item['Key'])}
        valid_file_set = [[utils.dict_append(self.extract_info_with_bucket(item['Key'], lmt_src_loc_bucket),item)] 
                          for item in list_page if  self.is_valid_file(key=item['Key']) ]
        
        log.info("Creating Thread pool executer to process list page")
        collected = NIO.decorated_run_io(task=self.file_process_task, task_n_args_list=valid_file_set, 
                                         max_workers=max_workers,)
        log.info("list page processed!")
    #     return collected
        return {'invalid_files_set' : invalid_files_set,
                'invalid_schema_files': self.grouped_set_of_flags(collected, flag_dict_type='invalid_schema_files'),
                'schema_tg_dict': self.grouped_flag_dict(collected, flag_dict_type='schema_tg_dict'),
                'target_tg_dict': self.grouped_flag_dict(collected, flag_dict_type='target_tg_dict'),
                'new_tg_schema_dict': self.grouped_flag_dict(collected, flag_dict_type='new_tg_schema_dict'),
                'new_tg_files_dict' : self.grouped_tg(collected, 'new_tg_files_dict'),
                'existing_tg_files_dict' : self.grouped_tg(collected, 'existing_tg_files_dict')
               }


    def extract_src_detail(self, max_workers=10, maxKeysPerReq=3, is_kernal_thread=False):
        s3_ops = self.s3_ops_src
        lmt_src = self.lmt_src
        
        '''  Intialising exist taxonomy data  which can be used '''
        tg_data_dict = self.extract_data_detail()
        self.tg_data_schema_dict = tg_data_dict['tg_data_schema_dict']
        self.tg_data_files_dict = tg_data_dict['tg_data_files_dict']
        
        lmt_src_loc_detail = s3_ops.get_bucket_name(lmt_src)
        lmt_src_loc_bucket = lmt_src_loc_detail['bucket']
        lmt_src_loc_key = lmt_src_loc_detail['key']
        page_generator = s3_ops.list_gen(lmt_src_loc_bucket, lmt_src_loc_key, maxKeysPerReq=maxKeysPerReq, )
        page_args_generator = ([page] for page in page_generator)
        #list_page = [i for i in page_generator][0]
        collected = NIO.decorated_run_with_args_generator(task=self.src_list_page_process_task, 
                                                          args_generator=page_args_generator, 
                                                          is_kernal_thread=is_kernal_thread, 
                                                          max_workers=max_workers)

        return {
                'invalid_files_set' : self.grouped_set_of_flags(collected, flag_dict_type='invalid_files_set'),
                'invalid_schema_files': self.grouped_set_of_flags(collected, flag_dict_type='invalid_schema_files'),
                'schema_tg_dict': self.grouped_set_of_flags_dict(collected, flag_dict_type='schema_tg_dict'),
                'target_tg_dict': self.grouped_set_of_flags_dict(collected, flag_dict_type='target_tg_dict'),
                'new_tg_schema_dict': self.grouped_set_of_flags_dict(collected, flag_dict_type='new_tg_schema_dict'),
                'new_tg_files_dict' : self.grouped_tg(collected, 'new_tg_files_dict'),
                'existing_tg_files_dict' : self.grouped_tg(collected, 'existing_tg_files_dict')
               }


    def extract_src_detail1(self, max_workers=10, maxKeysPerReq=3, is_kernal_thread=False):
        s3_ops = self.s3_ops_src
        lmt_src = self.lmt_src
        
        '''  Intialising exist taxonomy data  which can be used '''
        tg_data_dict = self.extract_data_detail()
        self.tg_data_schema_dict = tg_data_dict['tg_data_schema_dict']
        self.tg_data_files_dict = tg_data_dict['tg_data_files_dict']

        lmt_src_loc_detail = s3_ops.get_bucket_name(lmt_src)
        lmt_src_loc_bucket = lmt_src_loc_detail['bucket']
        lmt_src_loc_key = lmt_src_loc_detail['key']
        page_generator = s3_ops.list_gen(lmt_src_loc_bucket, lmt_src_loc_key, maxKeysPerReq=maxKeysPerReq, )
        page_args_generator = ([page] for page in page_generator)
        #list_page = [i for i in page_generator][0]
        collected = NIO.decorated_run_with_args_generator(task=self.src_list_page_process_task, 
                                                          args_generator=page_args_generator, 
                                                          is_kernal_thread=is_kernal_thread, 
                                                          max_workers=max_workers) 
        return collected

    
def extract_src_detail(self, max_workers=10, maxKeysPerReq=3, is_kernal_thread=False):
#         s3_ops = self.s3_ops_src
        lmt_src = self.lmt_src
        
        '''  Intialising exist taxonomy data  which can be used '''
        tg_data_dict = {'tg_data_schema_dict' : {}, 'tg_data_files_dict': {}}
        #tg_data_dict = self.extract_data_detail()
        self.tg_data_schema_dict = tg_data_dict['tg_data_schema_dict']
        self.tg_data_files_dict = tg_data_dict['tg_data_files_dict']

        lmt_src_loc_detail = s3_ops.get_bucket_name(lmt_src)
        lmt_src_loc_bucket = lmt_src_loc_detail['bucket']
        lmt_src_loc_key = lmt_src_loc_detail['key']
        page_generator = s3_ops.list_gen(lmt_src_loc_bucket, lmt_src_loc_key, maxKeysPerReq=maxKeysPerReq, )
        page_args_generator = ([page] for page in page_generator)
        #list_page = [i for i in page_generator][0]
        collected = NIO.decorated_run_with_args_generator(task=self.src_list_page_process_task, 
                                                          args_generator=page_args_generator, 
                                                          is_kernal_thread=is_kernal_thread, 
                                                          max_workers=max_workers) 
        return collected
    
    
    
if __name__ == '__main__':
    
    numeric_level = getattr(logging, 'DEBUG', None)
    stdout_handler = logging.StreamHandler(sys.stdout)
    logging.basicConfig(level=numeric_level,
                            format='%(asctime)s %(levelname)s %(name)s: %(message)s',
                            handlers=[stdout_handler])
    tcAPI = Taxonomy_CS_API()
    a = taxonomy_cs_api.extract_src_detail(tcAPI, is_kernal_thread=True)
    print("a" + str(a))