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

class Load:

    logger = logging.getLogger(__name__)
    
    def __init__(self, src_table:str, dest_db: str=None, dest_table=None):

        self.logger = logging.getLogger(__name__)

        self.src_db=System_Prop.staging_db
        self.src_table=src_table
        if dest_db is not None:
            self.dest_db = dest_db
        else: self.dest_db: str = System_Prop.staging_db
        if dest_table is not None:
           self.mta_tv_impressions = dest_table 
        else: self.mta_tv_impressions = TableName.mta_tv_impressions

    def create_load_script(self):
        
        db=self.dest_db
        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.hive_settings) \
                .append(Transform_HQL_Const.mta_tv_impression, db=db,
                        table=self.mta_tv_impressions,
                        tv_impressions=self.src_table) \

        load_script = hql_builder.value()
        self.logger.debug("\n\n Load Script: \n\n"+load_script)
        return load_script
    

    @DeprecationWarning
    def create_load_using_tables_script(self):

        db=self.dest_db
        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.hive_settings) \
                .append(Transform_HQL_Const.mta_tv_impression, db=db,
                        table=self.mta_tv_impressions,
                        tv_impressions=self.src_table) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.src_table) \

        load_script = hql_builder.value()
        self.logger.debug("\n\n Load Script: \n\n"+load_script)
        return load_script

    @DeprecationWarning
    def create_load_using_views_script(self):

        db=self.dest_db
        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.hive_settings) \
                .append(Transform_HQL_Const.mta_tv_impression, db=db,
                        table=self.mta_tv_impressions,
                        tv_impressions=self.src_table) \

        load_script = hql_builder.value()
        self.logger.debug("\n\n Load Script: \n\n"+load_script)
        return load_script

    @classmethod
    def create_result_table_script(cls,  result_loc, dest_db=None, table=None):

        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.create_mta_tv_impressions_table, db=dest_db, 
                        table=table,
                        location=result_loc) \

        script = hql_builder.value()
        cls.logger.info("\n\n Script: \n\n"+script)
        return script

    @classmethod
    def create_result_table(cls, result_loc, dest_db: str=None, 
                            started_on=int(time.time()), dry_run=False, mock_run=False):
        
        mta_tv_impressions = "{}_{}".format(TableName.mta_tv_impressions, str(started_on))
        hive_script = cls.create_result_table_script(result_loc, dest_db, table=mta_tv_impressions)
        if not dry_run and not mock_run:
            cls.logger.debug("\n\n Creating a External table: {} for result. \n\n".format(mta_tv_impressions))
            SQL.run_query(query=hive_script, raw=True, return_data=False)
        return mta_tv_impressions