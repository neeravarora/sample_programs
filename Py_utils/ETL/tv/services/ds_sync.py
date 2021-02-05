import tv.py_path
import tv.path_resolver
import os, sys
import re
import time
import logging
import traceback
from datetime import datetime

from tv.configs import Config
from tv.ds_metadata import System_Prop, TableName, S3Location
from tv.hive_script import HQL_Const, HQLScript_Builder
from tv.transform_script import Transform_HQL_Const
from tv.extract_script import Extract_HQL_Const

#from tv.util.s3_utils import S3
from tv.util.neucmd.neuhadoop import S3

from tv.util.neucmd.neusql import SQL
from tv.util import date_utils
from tv.util import log_util
from tv.states import StateService
from tv.stats.stats import StatUtil
from tv.util.notification_service import NotificationService

class DBMetadataSync:
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = System_Prop.staging_db

    def output_sync(self, dry_run=True, mock_run=True):
        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.ext_table_repair, db=self.db, table=TableName.mta_tv_impressions)
        sync_query = hql_builder.value()
        self.logger.info("\n\n Generated Impressions sync query: \n\n {} \n".format(sync_query))

        if mock_run or dry_run or sync_query == '':
            return sync_query
        self.logger.debug("\n\n Generated Impressions sync query is executing: \n\n")
        SQL.run_query(query=sync_query, raw=True, return_data=False)


    def sync(self, dry_run=True, mock_run=True):
        if mock_run: return ''
        sync_query_script = self.sync_script()

        if dry_run or sync_query_script == '':
            return sync_query_script
        self.logger.debug("\n\n DB Metadata sync query script is executing: \n\n")
        SQL.run_query(query=sync_query_script, raw=True, return_data=False)


    def sync_script(self):
        db = self.db
        self.logger.debug("db: "+db)
        #response = S3.list_s3_files_kantar(S3Location.kantar_tvweekly_s3_root)
        response = S3.list_kantar_files(S3Location.kantar_tvweekly_s3_root)
        available_metadata = S3.get_kantar_partition_metadata(response)
        
        existing_kantar_metadata = self.get_existing_metadata(db, TableName.kantar_log)
        existing_creative_metadata = self.get_existing_metadata(db, TableName.creative)

        self.logger.debug("kantar_add_partition_delta:\n")
        kantar_add_partition_delta = self.__delta(available_metadata, existing_kantar_metadata)
        self.logger.debug("creative_add_partition_delta:\n")
        creative_add_partition_delta = self.__delta(available_metadata, existing_creative_metadata)
        self.logger.debug("kantar_drop_partition_delta:\n")
        kantar_drop_partition_delta = self.__delta(existing_kantar_metadata, available_metadata)
        self.logger.debug("creative_drop_partition_delta:\n")
        creative_drop_partition_delta = self.__delta(existing_creative_metadata, available_metadata)


        kantar_add_partitions_script = self.generate_kantar_add_partitions(db, TableName.kantar_log, kantar_add_partition_delta)

        kantar_drop_partitions_script = self.generate_kantar_drop_partitions(db, TableName.kantar_log, kantar_drop_partition_delta)

        creative_add_partitions_script = self.generate_creative_add_partitions(db, TableName.creative, creative_add_partition_delta)

        creative_drop_partitions_script = self.generate_creative_drop_partitions(db, TableName.creative, creative_drop_partition_delta)


        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.hive_settings) \
                    .append(kantar_add_partitions_script) \
                    .append(kantar_drop_partitions_script) \
                    .append(creative_add_partitions_script) \
                    .append(creative_drop_partitions_script) \
                    .append(HQL_Const.ext_table_repair, db=db, table=TableName.tivo_log) \
                    .append(HQL_Const.ext_table_repair, db=db, table=TableName.experian_mango) \

#                     .append(HQL_Const.ext_table_repair, db=db, table=TableName.mta_tv_impressions) \
#                     .append(HQL_Const.ext_table_repair, db=db, table=TableName.kantar_log) \
#                     .append(HQL_Const.ext_table_repair, db=db, table=TableName.creative) \

        db_sync_query = hql_builder.value()
        self.logger.info("==================================================")
        self.logger.info("DB Metadata Sync script: \n\n"+db_sync_query)
        self.logger.info("==================================================")
        return db_sync_query



    def get_existing_metadata(self, db:str, table_name: str):
        show_partition_query = HQL_Const.show_partitions.format(db=db, table=table_name)
        df = SQL.run_query(query=show_partition_query, return_data=True)
        return self.get_partition_metadata(df)


    def __delta(self, dict1: dict, dict2: dict):
        delta : dict = {}
        for key, value in dict1.items():
            if key in dict2:
                val_delta = value.difference(dict2[key])
                if len(val_delta) > 0:
                        delta[key] = val_delta
            else:
                delta[key] = value
        self.logger.debug("__delta()\n Res :\n"+str(delta))
        return delta

    def get_partition_metadata(self, df):
        partition_data = df['partition']
        regex = '^year=([0-9]{4}?)/week=([0-9]{1,2}?)(/file_name=.*)??$'
        res_dict: dict = {}
        def fun2(x: str, res_dict: dict = {}):
            if re.match(regex, x):
                matched = re.findall(regex, x)
                year = matched[0][0]
                week = matched[0][1]
                if year not in res_dict:
                        res_dict[year]=set()
                res_dict[year].add(week)
        list(filter(lambda x: fun2(x, res_dict), partition_data))
        self.logger.debug("get_partition_metadata()\n Res :\n"+str(res_dict))
        return res_dict


    def generate_kantar_add_partitions(self, db, table, metadata:dict):
        hql_builder: HQLScript_Builder = HQLScript_Builder()
        for year, weeks in metadata.items():
            for w in weeks:
                if len(w) == 1:
                    w_loc = '0{}'.format(w)
                else:
                    w_loc = w
                hql_builder.append_expanded_queries(HQL_Const.add_partition_for_kantar, 
                            db=db, 
                            table=table,
                            year=year,
                            week=w,
                            file_name=S3Location.kantar_log_files,
                            partition_location=S3Location.kantar_log_locations(year, w_loc)) \
        
        return hql_builder.value()

    def generate_kantar_drop_partitions(self, db, table, metadata:dict):
        return self.__generate_kantar_alter__(HQL_Const.drop_partition_for_kantar, db, table, metadata)

    def generate_creative_add_partitions(self, db, table, metadata:dict):
        return self.__generate_kantar_alter__(HQL_Const.add_partition_for_creative, db, table, metadata)

    def generate_creative_drop_partitions(self, db, table, metadata:dict):
        return self.__generate_kantar_alter__(HQL_Const.drop_partition_for_creative, db, table, metadata)

    def __generate_kantar_alter__(self, query, db, table, metadata:dict):
        hql_builder: HQLScript_Builder = HQLScript_Builder()
        for year, weeks in metadata.items():
            for w in weeks:
                if len(w) == 1:
                    w_loc = '0{}'.format(w)
                else:
                    w_loc = w
                hql_builder.append_expanded_queries(query=query, 
                            db=db, 
                            table=table,
                            year=year,
                            week=w,
                            partition_location=S3Location.kantar_creative_location(year, w_loc)) \
        
        return hql_builder.value()