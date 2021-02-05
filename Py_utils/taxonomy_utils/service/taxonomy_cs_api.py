import sys, os, inspect, re
import time, logging
import pandas as pd 
from collections import defaultdict

from libs.s3_ops import S3_OPs
from libs.s3_stream import S3Stream
from libs.configs import Config
from libs.nio_executor import NIO
from libs import utils
from libs import xml_writer 
from libs import decorator
from model.models import Taxonomy_Grp


TG_EXTRACT_REGEX = '^.*?/([a-zA-Z]+\-?[0-9]*)/$'
FILE_EXTRACT_REGEX = '^.*/([a-zA-Z0-9.\-_]{0,255}.csv)$'  #'^.*?/([a-zA-Z]+\-?[0-9]{0-255}.csv)$'

KEY_REGEX = '^[Kk]ey_[A-Za-z0-9_]{2,30}$'
TARGET_REGEX = '^[Tt]arget_[A-Za-z0-9_]{2,30}$'

TARGET_EXTRACT_REGEX ='^.*,?(target_[A-Za-z0-9_-]+).*$'
TARGET_EXTRACT_2_REGEX ='(target_[A-Za-z0-9_-]+)'

VALID_FILE_KEY_REGEX = '^(.*/([a-zA-Z]+\-?[0-9]*)?/)?(([a-zA-Z]+\-?[0-9]*?)_([0-9]{4}-[0-9]{2}-[0-9]{2}?)_([a-zA-Z0-9.\-_]+?).csv?)$'



class Taxonomy_CS_API:
    


    
    def __init__(self, 
                 lmt_src = '../taxonomy_cs/lmt/input/', 
                 lmt_data = '../taxonomy_cs/lmt/data/', 
                 config = '/home/vbhargava/feature_test0/msaction_backend/customers/raj_ford_test/common/config/inputs/platform_config.xml', 
                 config_input_loc = '/home/vbhargava/feature_test0/temp/taxo_config_xmls/', 
                 config_file_name = 'test.xml', 
                 dn_version = '12.1', 
                 dry_run=False):
        
        log = logging.getLogger('Taxonomy_CS_API Init')
        config_data = Config.get_qubole_config(config)
        
        self.ACCESS_KEY_DATA=config_data['access_key']
        self.SECRET_KEY_DATA=config_data['secret_key']
        self.ACCESS_KEY_SRC=config_data['access_key']
        self.SECRET_KEY_SRC=config_data['secret_key']
        
        self.lmt_src = utils.path_resolve(config_data['s3_location'], lmt_src)
        self.lmt_data = utils.path_resolve(config_data['s3_location'], lmt_data)
        log.info('lmt_src : {}'.format(self.lmt_src))
        log.info('lmt_data : {}'.format(self.lmt_data))
        
        self.s3_ops_src = S3_OPs(self.ACCESS_KEY_SRC, self.SECRET_KEY_SRC)
        self.s3_ops_data = S3_OPs(self.ACCESS_KEY_DATA, self.SECRET_KEY_DATA)
        
        self.s3_stream_src = S3Stream(self.ACCESS_KEY_SRC, self.SECRET_KEY_SRC)
        self.s3_stream_data = S3Stream(self.ACCESS_KEY_DATA, self.SECRET_KEY_DATA)
        
        self.config_input_loc = config_input_loc
        self.config_file_name = config_file_name
        self.dn_version = dn_version
        self.dry_run = dry_run

    
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

    def validate_schema(self, schema, KEY_REGEX=KEY_REGEX, TARGET_REGEX=TARGET_REGEX):
        if schema=='': 
            return {'IsValid' : False, 'schema': schema, 'message' : "Schema shouldn't be empty"}
        tokens = schema.split(',')
        if len(tokens) < 2:
            return {'IsValid' : False, 'schema': schema, 'message' : "Schema should have at least 2 columns"}
        
        key_cnt = 0
        target_cnt = 0
        invalid_headers = []
        columns = defaultdict(list)
        res = {}
        target_cols_set = set()
        key_cols_set = set()
        for t in tokens:
            t = t.strip()
            if re.match(TARGET_REGEX, t):
                target_cnt = target_cnt + 1
                target_cols_set.add(t)
            elif re.match(KEY_REGEX, t):
                key_cnt = key_cnt + 1
                key_cols_set.add(t)
            else:
                invalid_headers.append(t)
            columns[t.lower()].append(1)

        error_msgs=[]
        if target_cnt < 1 :
            error_msgs.append("At least one Target column is required!")
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
                'TargetColsSet' : target_cols_set, 'KeyColsSet' : key_cols_set}

    
    @decorator.elapsed_time(func_name='extract_data_detail')
    def extract_data_detail(self, max_workers=25):
        '''
        Valid data Taxonomy Grps
    
        '''
        
        self.titled('..Extract DATA Details..')
        s3_ops = self.s3_ops_data
        
        lmt_data_loc_detail = s3_ops.get_bucket_name(self.lmt_data)
        lmt_data_loc_bucket = lmt_data_loc_detail['bucket']
        lmt_data_loc_key = lmt_data_loc_detail['key']
        valid_tg_list_res = s3_ops.list_subdirs(lmt_data_loc_bucket,lmt_data_loc_key,)
        if len(valid_tg_list_res) <=0 :
            return {'tg_data_schema_dict': {}, 
                    'tg_data_files_dict' :{}, 
                    'target_data_tg_dict': {}}

        valid_tgrp_loc_list = [[self.tg_by_prefix(item['Prefix']), 
                                '{}{}/'.format(self.lmt_data, self.tg_by_prefix(item['Prefix']))] 
                               for item in valid_tg_list_res]

        collected = NIO.decorated_run_io(task=self.get_data_n_schema_task, task_n_args_list=valid_tgrp_loc_list, 
                                         max_workers=max_workers,)

        tg_data_schema_dict = {k:self.extract_schema(v)  for item in collected for k, v in item['result']['schema'].items()}
#         tg_data_files_dict = {k:{u['Key']:u for u in v } for item in collected for k, v in item['result']['files'].items()}
        tg_data_files_dict = {k:{self.filename_by_key(u['Key']):u for u in v } 
                              for item in collected for k, v in item['result']['files'].items()}
    
        #target_data_tg_dict = {re.findall(TARGET_EXTRACT_REGEX,V)[0]: K for K, V in tg_data_schema_dict.items()}

        target_data_tg_dict = {target : K for K, V in tg_data_schema_dict.items() 
                           for target in re.findall(TARGET_EXTRACT_2_REGEX,V)}
    
        return {'tg_data_schema_dict': tg_data_schema_dict, 
                'tg_data_files_dict' :tg_data_files_dict, 
                'target_data_tg_dict': target_data_tg_dict}
    


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
        f_gen2 = (collect[K].update(V)  for item in f_gen for K, V in item.items())
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
            tg = src_file_details['FileGrp']
            file_name = src_file_details['FileName']

            if tg_data_schema_dict.get(tg) is None or tg_data_schema_dict.get(tg) != validate_res['Schema']:
                new_tg_schema_dict[tg] = validate_res['Schema']
                new_tg_files_dict[tg] = {file_name: src_file_details}
            else:
                existing_tg_files_dict[tg] = {file_name: src_file_details}

            src_file_details['Schema'] = validate_res['Schema']
            schema_tg_dict[validate_res['Schema']] = tg
            #target_tg_dict[validate_res['TargetCol']] = tg
            target_tg_dict = {target:tg for target in validate_res['TargetColsSet']}

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

    @decorator.elapsed_time(func_name="extract_src_detail")
    def extract_src_detail(self, max_workers=10, maxKeysPerReq=3, is_kernal_thread=False):
        self.titled('..Extract SRC Details..')
        s3_ops = self.s3_ops_src
        lmt_src = self.lmt_src
        
        '''  Intialising exist taxonomy data  which can be used '''
        tg_data_dict = self.extract_data_detail()
        self.tg_data_schema_dict = tg_data_dict['tg_data_schema_dict']
        self.tg_data_files_dict = tg_data_dict['tg_data_files_dict']
        self.target_data_tg_dict = tg_data_dict['target_data_tg_dict']
        
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


    ''' E.g. 
        s3_copy_into_data_loc_task('tg5', 'tg5_2020-11-01_ford.csv', 'taxonomy_cs/test1/src/tg5_2020-11-01_ford.csv', 48 )
        s3_remove_at_data_loc_task('taxonomy_cs/test1/data/tg2/tg2_2020-11-01_ford.csv')
    '''
    def s3_copy_into_data_loc_task(self, tg, file_name, src_file, src_size, dry_run=True):
        if self.dry_run is not None:
            dry_run = self.dry_run
            
        #     data_file_loc_detail = s3_ops.get_bucket_name(lmt_data)
        log = logging.getLogger('s3_copy_into_data_loc_task')
        s3_ops = self.s3_ops_src
        s3_ops_dest = self.s3_ops_data
        src_file_loc_detail = s3_ops.get_bucket_name(self.lmt_src)
        src_s3 = 's3://{}/{}'.format(src_file_loc_detail['bucket'], src_file)
        dest_s3 = '{}{}/{}'.format(self.lmt_data,tg, file_name)
        if dry_run:
            log.info("[dry_run]: S3 copy from {} to {}".format(src_s3, dest_s3))
        else:
            log.info("[Inprgress] S3 copy from {} to {}".format(src_s3, dest_s3))
            s3_ops_dest.copy(src=src_s3, dest = dest_s3, src_size=src_size)
            log.info("[Completed] S3 copy from {} to {}".format(src_s3, dest_s3))
        return 'Copied Successfully! by task'


    def s3_remove_at_data_loc_task(self, file,  dry_run=True):
        if self.dry_run is not None:
            dry_run = self.dry_run
        log = logging.getLogger('s3_remove_at_data_loc_task')
        s3_ops = self.s3_ops_data
        data_file_loc_detail = s3_ops.get_bucket_name(self.lmt_data)
    #     src_file_loc_detail = s3_ops.get_bucket_name(lmt_src)
    #     src_s3 = 's3://{}/{}'.format(lmt_src, src_file)
        
        file_loc = 's3://{}/{}'.format(data_file_loc_detail['bucket'], file)

        if dry_run:
            log.info("[dry_run]: S3 delete from {} ".format(file_loc))
        else:
            log.info("[Inprgress] S3 delete from {} ".format(file_loc))
            s3_ops.delete_file(data_file_loc_detail['bucket'], file)
            log.info("[Completed] S3 delete from {} ".format(file_loc))
        return 'Deleted Successfully! by task'


    def key_target_splitter(self, schema = '', KEY_REGEX=KEY_REGEX, TARGET_REGEX=TARGET_REGEX):
        
        # KEY_REGEX = '^[Kk]ey_[A-Za-z0-9_]{2,30}$'
        # TARGET_REGEX = '^[Tt]arget_[A-Za-z0-9_]{2,30}$'
        tokens = schema.split(',')
        key_cols = []
        target_cols = []
        for t in tokens:
            t = t.strip()
            if re.match(TARGET_REGEX, t):
                target_cols.append(t)
            elif re.match(KEY_REGEX, t):
                key_cols.append(t)
            else:
                raise Exception("Not a valid schema")
        return [{'target_cols': target_cols, 'key_cols' : key_cols}]


    @decorator.box_logged
    def log_report(self, list_of_row_dict=[], columns:list=[], header_align = 'left', 
                   sort_by= None, ascending = True, report_title='', ):
        pd.set_option("display.colheader_justify", header_align)
        
        if report_title != '': 
            self.report_titled(report_title)

        if len(list_of_row_dict) == 0:
            logging.info("")
            logging.info("      No Records Found!! (Result set is Empty)")
            logging.info("")
            return

        df = pd.DataFrame(list_of_row_dict, columns=columns) 
        if sort_by is not None:
            df = df.sort_values(by=sort_by, ascending=ascending)
            df = df.reset_index()
            df = df.drop(columns=['index'])
        #df = df.set_index(' **      ' + df.index.astype(str) )
        df = df.rename(' **      {}'.format)
    #     df1 = df.reindex(columns=['Taxonomy_Grp','File','Date', 'Schema'])
        #df[df.columns[new_order]]
        #df = df.transpose()
       
        logging.info("")
        logging.info("")
        logging.info(str(df))
        logging.info("")
        logging.info("")




    @decorator.box_titled
    def report_titled(self, title:str=''):
        logging.info("")
        logging.info("    "+title)
        logging.info("")
        
    @decorator.box_logged
    def titled(self, title:str=''):
        logging.info("")
        logging.info("            "+title)
        logging.info("")
        
    @decorator.elapsed_time(func_name="delta")
    def delta(self):
        log = logging.getLogger(__name__)
        self.titled('..Delta..')
        dn_version = self.dn_version
        config_input_loc = self.config_input_loc
        config_file_name = self.config_file_name
        lmt_data = self.lmt_data
        src_delta = self.extract_src_detail()
        
        ''' Extract Info needed to expose configs and show in logs and reports'''

        invalid_files_set = src_delta['invalid_files_set']
        invalid_schema_files = src_delta['invalid_schema_files']
        
        tg_data_schema_dict = self.tg_data_schema_dict
        tg_data_files_dict = self.tg_data_files_dict
        target_data_tg_dict = self.target_data_tg_dict

        tg_data = { k for k in tg_data_files_dict.keys()}
        tg_existing = { k for k in src_delta['existing_tg_files_dict'].keys()}
        tg_new ={ k for k in src_delta['new_tg_files_dict'].keys()}
        tg_all = tg_new.union(tg_existing)

        many_tg4schema_check_gen = (v for k, v in src_delta['schema_tg_dict'].items() if len(v) > 1)
        many_tg4target_check_gen = (v for k, v in src_delta['target_tg_dict'].items() if len(v) > 1)
        newTg4schema = {k for k, v in src_delta['new_tg_schema_dict'].items() if len(v) > 1}


        tg4schema = set()
        [tg4schema.update(i) for i in many_tg4schema_check_gen]
        tg4target = set()
        [tg4target.update(i) for i in many_tg4target_check_gen]


        # invalid_tg_with_dup_schema = (tg4schema.union(newTg4schema)).difference(tg_existing)

        # invalid_tg_with_dup_target = tg4target.difference(tg_existing)

        # invalid_tg_all = invalid_tg_with_dup_schema.union(invalid_tg_with_dup_target)

        invalid_tg_all = newTg4schema.difference(tg_existing)

        tg_delta = tg_new.difference(invalid_tg_all)

        tg_delta_create = tg_delta.difference(tg_data)

        tg_delta_drop_n_create = (tg_delta.intersection(tg_data)).difference(tg_existing)

        tg_dropped = tg_data.difference(tg_all)

        tg_dropped_all = tg_dropped.union(tg_delta_drop_n_create)
        tg_create_all = tg_delta_create.union(tg_delta_drop_n_create)
        
        #--
        invalid_tg_with_dup_schema = (tg4schema.union(newTg4schema)).difference(tg_existing)



        ''' File Sync'''
        files_to_be_dropped = [[f['Key']] for i in  tg_dropped_all 
                               for fn, f in tg_data_files_dict.get(i).items()]
        files_not_retained_existing_tg =[[tg_data_files_dict.get(tg).get(fn)['Key']]
                                         for tg in tg_existing 
                                         for fn in set(tg_data_files_dict.get(tg).keys()).difference(
                                             set(src_delta['existing_tg_files_dict'].get(tg).keys()))]

        file_drop_args = []
        file_drop_args.extend(files_to_be_dropped )
        file_drop_args.extend(files_not_retained_existing_tg )

        '''  Debug stuff'''
        log.debug("test=> tg_data="+str(tg_data))
        log.debug("test=> tg_existing="+str(tg_existing))
        log.debug("test=> tg_new="+str(tg_new))
        log.debug("test=> tg_all="+str(tg_all))
        
        
        log.debug("test=> tg_delta="+str(tg_delta))
        log.debug("test=> tg_delta_create="+str(tg_delta_create))
        log.debug("test=> tg_delta_drop_n_create="+str(tg_delta_drop_n_create))
        log.debug("test=> tg_dropped="+str(tg_dropped))
        log.debug("test=> tg_dropped_all="+str(tg_dropped_all))
        log.debug("test=> tg_create_all"+str(tg_create_all))
        
        files_to_be_created = [[i, f['FileName'], f['Key'], f['Size']] 
                               for i in  tg_create_all 
                               for fn, f in src_delta['new_tg_files_dict'].get(i).items()]
        files_to_be_copied = [[k, f_dict['FileName'], f_dict['Key'], f_dict['Size']] 
                              for k, v in src_delta['existing_tg_files_dict'].items() 
                              for f, f_dict in v.items()] #['Key']]
        file_copy_args = []
        file_copy_args.extend(files_to_be_created )
        file_copy_args.extend(files_to_be_copied )

        collected = NIO.decorated_run_io(task=self.s3_remove_at_data_loc_task, task_n_args_list=file_drop_args, 
                                         is_kernal_thread=False,)

        collected = NIO.decorated_run_io(task=self.s3_copy_into_data_loc_task, task_n_args_list=file_copy_args, 
                                         is_kernal_thread=False,)



        ''' Expose details to generate configs'''
        tg_create_all_n_schema = {tg: schema 
                                  for tg in tg_create_all 
                                  for schema in src_delta['new_tg_schema_dict'].get(tg)}
        tg_retain_all_n_schema = {tg: tg_data_schema_dict.get(tg) 
                                  for tg in tg_existing}
        tg_dropped_all_n_schema = {tg: tg_data_schema_dict.get(tg) 
                                   for tg in tg_dropped_all }


        '''New and Drop_n_create(With new attributes like schema) Taxonomy Grps'''
        exposed_tg_all = [Taxonomy_Grp(tg,schema_dict['key_cols'], schema_dict['target_cols'], lmt_data) 
                        for tg ,schema in tg_create_all_n_schema.items() 
                        for schema_dict in self.key_target_splitter(schema)]
        '''Retaining Taxonomy Grps with either NO CHANGES or Create and Drop some files in a retained group'''
        exposed_tg_all.extend([Taxonomy_Grp(tg,schema_dict['key_cols'], schema_dict['target_cols'], lmt_data) 
                            for tg ,schema in tg_retain_all_n_schema.items() 
                            for schema_dict in self.key_target_splitter(schema)])


        '''Dropped and Drop_n_create(With old attributes like schema) Taxonomy Grps'''
        exposed_dropped_tg_all = [Taxonomy_Grp(tg,schema_dict['key_cols'], schema_dict['target_cols'], lmt_data) 
                                for tg ,schema in tg_dropped_all_n_schema.items() 
                                for schema_dict in self.key_target_splitter(schema)]


        '''Exposed Tg grp with precedence order'''
        exposed_tg_name_list = [i.tg_name for i in exposed_tg_all]

        exposed_tg_name_ordered_list = sorted(exposed_tg_name_list, key=str.lower)

        
        self.titled('..Generating Output Config XML..')


        ''' Generating output config xml'''
        xml_writer.generate_output_config(exposed_tg_all, exposed_dropped_tg_all, exposed_tg_name_ordered_list, 
                                  dn_version, config_input_loc, config_file_name)
        
        ''' Generating Report'''
        self.gen_Report(exposed_tg_all = exposed_tg_all, 
               exposed_dropped_tg_all = exposed_dropped_tg_all,
            #    invalid_tg_with_dup_schema = invalid_tg_with_dup_schema, 
            #    invalid_tg_with_dup_target = invalid_tg_with_dup_target,
               invalid_new_tg_schema_conflict = invalid_tg_all,
               src_delta  = src_delta,
               tg_new  = tg_new, 
               tg_existing = tg_existing,
               invalid_files_set = invalid_files_set,
               invalid_schema_files = invalid_schema_files,
               tg_data_files_dict = tg_data_files_dict, 
               tg_data_schema_dict = tg_data_schema_dict, 
               tg_delta_drop_n_create = tg_delta_drop_n_create, 
               tg_dropped = tg_dropped,
               tg_delta_create = tg_delta_create
)
        
        
        
    @decorator.elapsed_time(func_name="gen_Report")   
    def gen_Report(self,
               exposed_tg_all = None, 
               exposed_dropped_tg_all = None,
            #    invalid_tg_with_dup_schema = None, 
            #    invalid_tg_with_dup_target = None,
               invalid_new_tg_schema_conflict = None,
               src_delta  = None,
               tg_new  = None, 
               tg_existing = None,
               invalid_files_set = None,
               invalid_schema_files = None,
               tg_data_files_dict = None, 
               tg_data_schema_dict = None, 
               tg_delta_drop_n_create = None,
               tg_dropped = None,
               tg_delta_create = None):

        
        ''' Report '''
        self.titled('..Reports..')

        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_colwidth', 1000)


#         tg_data_schema_dict = self.tg_data_schema_dict
#         tg_data_files_dict = self.tg_data_files_dict
#         target_data_tg_dict = self.target_data_tg_dict

        log_report = self.log_report
        extract_info = self.extract_info
        
        
        '''  Exposed TG Report  '''

        exposed_tg_report_data = [i.get_dict() for i in exposed_tg_all]
        report_title = 'Exposed TG Report'
        log_report(exposed_tg_report_data,  columns=['tg_name', 'key_cols', 'target_cols', 'location'], 
                sort_by='tg_name', report_title=report_title)


        '''  Exposed Dropped TG Report '''

        exposed_tg_dropped_report_data = [i.get_dict() for i in exposed_dropped_tg_all]
        report_title = 'Exposed Dropped TG Report'
        log_report(exposed_tg_dropped_report_data, columns=['tg_name', 'key_cols', 'target_cols', 'location'], 
                sort_by='tg_name', report_title=report_title)



        '''   Dropped TG Completely '''

        tg_dropped_rep_gen =(extract_info(f['Key']) for i in  tg_dropped for fn, f in tg_data_files_dict.get(i).items())
        tg_dropped_report_dict =  [{'Taxonomy_Grp':i['FileGrp'], 'File_Name':i['FileName'], 
                                    'Date':i['Date'], 'Schema': tg_data_schema_dict[i['FileGrp']] }
                                   for i in tg_dropped_rep_gen] 
        report_title = 'Dropped TG Completely'
        log_report(list_of_row_dict=tg_dropped_report_dict, columns=['Taxonomy_Grp','File_Name','Date', 'Schema'], 
                   sort_by = ['Taxonomy_Grp','Date'], report_title=report_title) 


        '''   Dropped TG to change schema '''

#         tg_drop_schema_change_rep = ((tg, tg_data_schema_dict.get(tg), schema_new, extract_info(f['Key'])) 
#                                      for tg in tg_delta_drop_n_create 
#                                      for schema_new in src_delta['new_tg_schema_dict'].get(tg)
#                                      for fn, f in tg_data_files_dict.get(tg).items())
#         tg_drop_schema_change_report_dict = [{'Grp' : i[0], 'File_Name': i[3]['FileName'], 
#                                               'Date': i[3]['Date'], 'Old_Schema' : i[1], 'New_Schema' : i[2]} 
#                                              for i in tg_drop_schema_change_rep]
#         report_title = 'Dropped TG to change schema'
#         log_report(list_of_row_dict=tg_drop_schema_change_report_dict, 
#                    columns=['Grp','File_Name','Date', 'Old_Schema', 'New_Schema'], 
#                    sort_by = ['Grp','Date'], report_title=report_title) 
        
        tg_drop_schema_change_rep = ((tg, tg_data_schema_dict.get(tg), schema_new, extract_info(f['Key']), 'Re-delivered') 
                                     if src_delta['new_tg_files_dict'].get(tg).get(fn) is not None 
                                     else (tg,  tg_data_schema_dict.get(tg),'NAN', extract_info(f['Key']), 'Dropped')
                             
                                     for tg in tg_delta_drop_n_create 
                                     for schema_new in src_delta['new_tg_schema_dict'].get(tg) 
                                     for fn, f in tg_data_files_dict.get(tg).items())
        tg_drop_schema_change_report_dict = [{'Grp' : i[0], 'File_Name': i[3]['FileName'], 'Date': i[3]['Date'], 
                                              'Old_Schema' : i[1], 'New_Schema' : i[2], 'Desc' : i[4]} 
                                             for i in tg_drop_schema_change_rep]
        report_title = 'Dropped TG to change schema'
        log_report(list_of_row_dict=tg_drop_schema_change_report_dict,
                   columns=['Grp','File_Name','Date', 'Old_Schema', 'New_Schema', 'Desc'], 
                   sort_by = ['Grp','Date'], report_title=report_title)  




        '''   Created TG Absolute New '''

        tg_newly_created_report_data = [ {'Taxonomy_Grp':f['FileGrp'], 'File_Name':f['FileName'], 
                                          'Date':f['Date'], 'Schema': f['Schema'] } 
                                        for i in  tg_delta_create for fn, f in src_delta['new_tg_files_dict'].get(i).items()]
        report_title = 'Created TG Absolute New'
        log_report(tg_newly_created_report_data, columns=['Taxonomy_Grp','File_Name','Date', 'Schema'], 
                   sort_by = ['Taxonomy_Grp','Date'], report_title=report_title) 


        '''   Created TG to change schema(with new Schema) '''

        tg_re_created_schema_change_rep = ((tg, tg_data_schema_dict.get(tg), f['Schema'], 
                                            extract_info(f['Key']), 'Re-delivered') 
                                           if tg_data_files_dict.get(tg).get(fn) is not None 
                                           else (tg, 'NAN', f['Schema'], extract_info(f['Key']), 'New File')

                                           for tg in tg_delta_drop_n_create 
                                           #for schema_new in src_delta['new_tg_schema_dict'].get(tg) 

                                           for fn, f in src_delta['new_tg_files_dict'].get(tg).items() 
                                           )

        tg_recreated_schema_change_report_dict = [{'Grp' : i[0], 'File_Name': i[3]['FileName'], 'Date': i[3]['Date'], 
                                                   'Old_Schema' : i[1], 'New_Schema' : i[2], 'Desc': i[4]} 
                                                  for i in tg_re_created_schema_change_rep]
        report_title = 'Created TG to change schema(with new Schema)'
        log_report(list_of_row_dict=tg_recreated_schema_change_report_dict, 
                   columns=['Grp','File_Name','Date', 'Old_Schema', 'New_Schema', 'Desc'], 
                   sort_by = ['Grp','Date'], report_title=report_title)


        '''   Retained TG with retained files, new files and dropped files '''

        tg_retained_report_data = [{'Taxonomy_Grp':f['FileGrp'], 'File_Name':f['FileName'], 'Date':f['Date'], 
                                    'Schema': f['Schema'], 'Desc' : 'Retained' }

                                    if tg_data_files_dict.get(tg).get(fn) is not None 
                                    else {'Taxonomy_Grp':f['FileGrp'], 'File_Name':f['FileName'], 'Date':f['Date'], 
                                          'Schema': f['Schema'], 'Desc' : 'New File' }
                                    for tg in tg_existing 
                                    for fn, f in src_delta['existing_tg_files_dict'].get(tg).items()]

        tg_retained_dropped_files = [extract_info(tg_data_files_dict.get(tg).get(fn)['Key']) 
                                     for tg in tg_existing 
                                     for fn in set(tg_data_files_dict.get(tg).keys()).difference(
                                         set(src_delta['existing_tg_files_dict'].get(tg).keys()))]


        tg_retained_dropped_files_report_data = [{'Taxonomy_Grp':f['FileGrp'], 'File_Name':f['FileName'], 'Date':f['Date'],
                                                  'Schema': tg_data_schema_dict[f['FileGrp']], 'Desc' : 'Dropped' }
                                                  for f in tg_retained_dropped_files]

        tg_retained_report_data.extend(tg_retained_dropped_files_report_data)
        report_title = 'Retained TG with retained files, new files and dropped files'
        log_report(tg_retained_report_data, columns=['Taxonomy_Grp','File_Name','Date', 'Schema', 'Desc'], 
                   header_align='left', sort_by = ['Taxonomy_Grp','Date'], report_title=report_title) 


        # '''  Invalid TG due to schema conflict/already used Report '''

        # invalid_tg_with_dup_schema_rep = [ {'Taxonomy_Grp':f['FileGrp'], 'File_Name':f['FileName'], 
        #                                     'Date':f['Date'], 'Schema': f['Schema'] }
        #                                   for i in  invalid_tg_with_dup_schema 
        #                                   for fn, f in src_delta['new_tg_files_dict'].get(i).items()]
        # report_title = 'Invalid TG due to schema conflict/already used Report'
        # log_report(invalid_tg_with_dup_schema_rep, columns=['Taxonomy_Grp','File_Name','Date', 'Schema'], 
        #            sort_by = ['Taxonomy_Grp','Date'], report_title=report_title) 


        # '''  Invalid TG due to Target Column conflict/already used Report '''

        # invalid_tg_with_dup_target_rep = [ {'Taxonomy_Grp':f['FileGrp'], 'File_Name':f['FileName'], 
        #                                     'Date':f['Date'], 'Schema': f['Schema'] } 
        #                                   for i in  invalid_tg_with_dup_target 
        #                                   for fn, f in src_delta['new_tg_files_dict'].get(i).items()]
        # report_title = 'Invalid TG due to Target Column conflict/already used Report'
        # log_report(invalid_tg_with_dup_target_rep, columns=['Taxonomy_Grp','File_Name','Date', 'Schema'], 
        #            sort_by = ['Taxonomy_Grp','Date'], report_title=report_title) 


        '''  Invalid New TG due to Schema conflict among files Report '''

        invalid_tg_with_schema_conflict_rep = [ {'Taxonomy_Grp':f['FileGrp'], 'File_Name':f['FileName'], 
                                                 'Date':f['Date'], 'Schema': f['Schema'] } 
                                               for i in  invalid_new_tg_schema_conflict 
                                               for fn, f in src_delta['new_tg_files_dict'].get(i).items()]

        report_title = 'Invalid New TG due to Schema conflict among files Report'
        log_report(invalid_tg_with_schema_conflict_rep, columns=['Taxonomy_Grp','File_Name','Date', 'Schema'], 
                sort_by = ['Taxonomy_Grp','Date'], report_title=report_title)


        ''' Invalid files from retained grp due to schema or target mismatch with previously delivered files for same'''

        partially_invalid_tg_set = tg_new.intersection(tg_existing)
        partially_invalid_tg_report = [ {'Taxonomy_Grp':f['FileGrp'], 'File_Name':f['FileName'], 'Date':f['Date'], 
                                         'Schema': f['Schema'], 'Grp_Schema': tg_data_schema_dict[i] } 
                                       for i in partially_invalid_tg_set  
                                       for fn, f in src_delta['new_tg_files_dict'].get(i).items()]
        report_title = 'Invalid files from retained grp due to schema or target mismatch'
        log_report(partially_invalid_tg_report, columns=['Taxonomy_Grp','File_Name','Date', 'Schema','Grp_Schema'], 
                   sort_by = ['Taxonomy_Grp','Date'], report_title=report_title) 


        '''   Invalid file not match with required file pattern '''

        invalid_files_report_data = [{'File_Name' : self.filename_by_key(i)} for i in invalid_files_set]
        report_title = 'Invalid file not match with required file pattern'
        log_report(invalid_files_report_data,  columns=['File_Name'], 
                   sort_by = ['File_Name'], report_title=report_title)


        '''   Invalid file not match with required schema pattern '''

        invalid_schema_files_rep_data = [{'File_Name' : self.filename_by_key(i[0]), 'Schema': i[1], 'Reason' : i[2]} 
                                         for i in invalid_schema_files]
        report_title = 'Invalid file not match with required schema pattern'
        log_report(invalid_schema_files_rep_data,  columns=['File_Name', 'Schema','Reason'], 
                   header_align='left', sort_by = ['File_Name'], report_title=report_title)








