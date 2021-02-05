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

class CreativeLmt:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.creative_validation_dict = {}
        self.ALREADY_UPDATED_MSG = 'Creative LMT is already updated till MTA TV impressions processed'
        self.MERGE_SUCCESS_MSG = 'Kantar creative changes from kantar week {} of {} \
                                    to kantar week {} of {} has been merged on Creative LMT'
        self.start_week_val = -1
        self.start_year_val = -1

    #@property
    def creative_validation_data(self) -> dict:
        return self.creative_validation_dict

    #@property
    def start_week(self) -> int:
        return self.start_week_val

    #@property
    def start_year(self) -> int:
        return self.start_year_val

    def setup_creative_lmt(self, kantar_end_year, kantar_end_week, started_on, dry_run=True, mock_run = True):
        if not mock_run and not dry_run:
            if kantar_end_week == 0:
                kantar_end_year = kantar_end_year - 1
                no_of_weeks = date_utils.num_of_weeks(kantar_end_year)
                kantar_end_week = no_of_weeks + 1
            else:
                kantar_end_week = kantar_end_week - 1

            StatUtil.save_creative_lmt_stat(kantar_end_year, kantar_end_week, started_on, dry_run=dry_run, mock_run=mock_run)
        self.logger.info("Creative LMT Setup is done from week {} of year {}.".format(kantar_end_week, kantar_end_year))


    def update_creative_lmt(self, kantar_end_year, kantar_end_week, started_on, dry_run=True, mock_run = True ):
        start_week = None
        start_year = None
        try:
            if not mock_run:
                self.logger.info("Creative LMT triggerd for week {} of year {}.".format(kantar_end_week, kantar_end_year))
                creative_lmt_stat = StatUtil.get_creative_lmt_stat(mock_run)
                if creative_lmt_stat is not None:
                    start_week = creative_lmt_stat['week']
                    start_year = creative_lmt_stat['year']
                    no_of_weeks = date_utils.num_of_weeks(start_year)
                    if start_week >= no_of_weeks - 1:
                        start_year = start_year + 1
                        start_week = 0
                    else:
                        start_week = start_week + 1

                    end_week = str(kantar_end_week)
                    end_year = str(kantar_end_year)
                else :
                    start_week = 10
                    start_year = 1971

                creative_transform_script = self.__create_creative_transform_script(start_year, end_year, 
                                                                                    start_week, end_week, started_on)
                if start_week > int(end_week) or  start_year > int(end_year):
                    self.logger.info("No new creative data available to update creative lmt")
                    self.logger.info("Creative lmt updated till week {} of year {}.\n\n".format
                                    (str(creative_lmt_stat['week']), str(creative_lmt_stat['year'])))
                    self.creative_validation_dict['msg'] = self.ALREADY_UPDATED_MSG
#                     raise Exception("Testing creative inside")
                    return

                self.creative_validation_dict['msg'] = self.MERGE_SUCCESS_MSG.format(start_week, start_year, end_week, end_year)
            else:
                start_week = 51
                start_year = 2018
                creative_transform_script = self.__create_creative_transform_script('2018', 
                                            '2020', '51', 
                                            '10', started_on)
                self.creative_validation_dict['msg'] = self.ALREADY_UPDATED_MSG

            self.logger.info("==================================================")
            self.logger.info("ETL Default Creative lmt Run: \n\n"+creative_transform_script)
            self.logger.info("==================================================")
            
#             raise Exception("Testing creative")
            if not dry_run and not mock_run:
                self.logger.debug("\n\n ETL Default trigger script is executing: \n\n")
                SQL.run_query(query=creative_transform_script, raw=True, return_data=False)
                StatUtil.save_creative_lmt_stat(end_year, end_week, started_on, dry_run=False, mock_run=False)
        except Exception as e:
            self.start_week_val = start_week
            self.start_year_val = start_year
            self.logger.error('creative lmt update has failed!!')
            raise e #Exception("creative lmt update has failed!!")
        

    def __create_creative_transform_script(self, start_year, end_year, start_week, end_week, started_on=int(time.time())):
        creative_staging = TableName.creative_staging.format(str(started_on))
        db=System_Prop.staging_db

        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.hive_settings) \
                .append(Transform_HQL_Const.kantar_creative_transform, db=db,
                        creative_lmt=TableName.creative_lmt,
                        staging_creative_lmt=creative_staging,
                        raw_creative=TableName.creative,
                        start_year=start_year,
                        end_year=end_year,
                        start_week=start_week,
                        end_week=end_week) \

        transform_script = hql_builder.value()
        self.logger.debug("\n\n Creative Transform Script: \n\n"+transform_script)
        return transform_script