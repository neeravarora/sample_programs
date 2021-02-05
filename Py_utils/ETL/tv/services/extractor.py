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


class Extractor:
    
    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self.db=System_Prop.staging_db
        self.location=System_Prop.staging_s3_loc
        self.tivo_validation_dict = {}
        self.kantar_validation_dict = {}
        self.experian_validation_dict = {}
        self.errored_msgs_list = []
        self.errored = False

    #@property
    def tivo_validation_data(self) -> dict:
        return self.tivo_validation_dict

    #@property
    def kantar_validation_data(self) -> dict:
        return self.kantar_validation_dict

    #@property
    def experian_validation_data(self) -> dict:
        return self.experian_validation_dict

    #@property
    def errored_msgs(self) -> list:
        return self.errored_msgs_list

    #@property
    def is_errored(self) -> bool:
        return self.errored

    def intialise(self, start_date: str, end_date: str, 
        experian_lookback : int=7,
        start_timestamp=int(time.time()),
        dry_run=True, mock_run = True):

        self.start_timestamp = start_timestamp
        self.start_timestamp_str = str(start_timestamp)
        self.start_date = date_utils.valid_date_fmt(start_date)
        self.end_date = date_utils.valid_date_fmt(end_date)
        self.logger.info("Extractor initiated to extract data in given date range.\n")
        self.logger.info("    Start date :  {}".format(self.start_date))
        self.logger.info("    End date   :  {}\n".format(self.end_date))

        '''Tivo log data files uncentainty handles''' 
        self.start_date_numeric = date_utils.convert_date_fmt(self.start_date)
        given_end_date_numeric = date_utils.convert_date_fmt(self.end_date)
        self.end_date_numeric = given_end_date_numeric

        if not mock_run:
            self.end_date_numeric = self.__extract_val_from_hive(query=Extract_HQL_Const.nearest_to_enddate_after_end_date,
                                        extracted_key = 'file_name', 
                                        db = self.db, 
                                        table=TableName.tivo_log,
                                        end_date = str(given_end_date_numeric))
            if self.end_date_numeric is None:
                self.end_date_numeric = self.__extract_val_from_hive(query=Extract_HQL_Const.nearest_to_enddate_in_date_range,
                                        extracted_key = 'file_name', 
                                        db = self.db, 
                                        table=TableName.tivo_log,
                                        start_date = str(self.start_date_numeric),
                                        end_date = str(given_end_date_numeric))

            # if self.end_date_numeric is None:
            #     self.logger.error("Insuffiecient data for tivo")
            #     assert False, 'Insuffiecient data for tivo'
            # self.end_date = date_utils.convert_date_fmt(str(self.end_date_numeric), current_fmt = '%Y%m%d', output_fmt = '%Y-%m-%d')

        # Tivo validation
        if self.end_date_numeric is not None:
            tivo_modifified_date = date_utils.convert_date_fmt(str(self.end_date_numeric), current_fmt = '%Y%m%d', output_fmt = '%Y-%m-%d')
            if int(given_end_date_numeric) >  int(self.end_date_numeric):
                self.tivo_validation_dict['start_date'] = self.start_date
                self.tivo_validation_dict['end_date'] = self.end_date
                self.tivo_validation_dict['description'] = "Tivo data missed for few days and it has assumed \
                        to be delivered as combined data in next available file with date {}.".format(self.end_date)
                self.logger.warn(self.tivo_validation_dict['description'])
                self.logger.warn("\nTV ETL Processing end_date is being modified to {} for this trigger due to tivo data\n".format(tivo_modifified_date))
                
            elif int(given_end_date_numeric) <  int(self.end_date_numeric):
                self.tivo_validation_dict['start_date'] = self.start_date
                self.tivo_validation_dict['end_date'] = self.end_date
                self.tivo_validation_dict['description'] = "Tivo data is not available till given end date. \
                        Hence TV ETL processing has modified."
                self.logger.warn(self.tivo_validation_dict['description'])
                self.logger.warn("\nTV ETL Processing end_date is being modified to {} for this trigger due to tivo data\n".format(tivo_modifified_date))
                
            else:
                self.tivo_validation_dict['start_date'] = self.start_date
                self.tivo_validation_dict['end_date'] = self.end_date
                self.tivo_validation_dict['description'] = "Tivo data is available for given date range."
                self.logger.info(self.tivo_validation_dict['description'])
            self.end_date = tivo_modifified_date #date_utils.convert_date_fmt(str(self.end_date_numeric), current_fmt = '%Y%m%d', output_fmt = '%Y-%m-%d')

        else:
            self.tivo_validation_dict['start_date'] = 'NA'
            self.tivo_validation_dict['end_date'] = 'NA'
            self.tivo_validation_dict['description'] = "Insuffiecient data for tivo in given time interval."
            self.errored = True
            self.errored_msgs_list.append("Insuffiecient data for tivo in given time interval.")
            self.logger.warn(self.tivo_validation_dict['description'])
        
        # # till hear

        kantar_start_year_week = date_utils.get_kantar_week_with_year(self.start_date, fmt = '%Y-%m-%d')
        kantar_end_year_week = date_utils.get_kantar_week_with_year(self.end_date, fmt = '%Y-%m-%d')
        self.kantar_year = kantar_start_year_week['year']

        self.kantar_start_week = kantar_start_year_week['week']
        self.kantar_end_week = kantar_end_year_week['week']

        ''' Kantar start and end week should be part of same year '''
        if self.kantar_year != kantar_end_year_week['year']:
            no_of_weeks = date_utils.num_of_weeks(self.kantar_year)
            self.end_date = date_utils.get_date_range_for_week(no_of_weeks, self.kantar_year)['week_end_date_str']
            self.kantar_end_week = no_of_weeks - 1
            self.logger.warn("TV ETL Processing End_date is being modified for this trigger due to year transition.")
            self.logger.warn("Kantar start and end week should be part of same year\n")
            self.logger.warn("Modified End Date : {}\n".format(self.end_date))

        
        ''' Kantar log validations''' 
        available_kantar_weeks = []
        start_week_flag = self.kantar_start_week
        if not mock_run:
            available_kantar_weeks = self.__extract_sorted_list_from_hive(
                            Extract_HQL_Const.kantar_weeks_exists_in_range, 'weeks', 
                            db = self.db, 
                            table=TableName.kantar_log,
                            start_year=self.kantar_year,
                            end_year=self.kantar_year,
                            start_week=self.kantar_start_week,
                            end_week=self.kantar_end_week)

            # if available_kantar_weeks is None:
            #     self.logger.error('System is not able to recognize Kantar weekly log')
            #     assert False, 'System is not able to recognize Kantar weekly log'

            if available_kantar_weeks is not None:
                for i in available_kantar_weeks:
                    if i == start_week_flag:
                        start_week_flag = start_week_flag +1
                        continue
                    break

                if self.kantar_end_week > start_week_flag - 1:
                    self.logger.warn("Insuffiecient data in Kantar for end week {}".format(self.kantar_end_week))
                    self.logger.warn("Kantar end week is being modified to {} for this trigger".format(start_week_flag - 1))

                    self.kantar_end_week = start_week_flag - 1
                    self.end_date = date_utils.get_date_range_for_kantar_week(self.kantar_end_week, 
                                            self.kantar_year)['week_end_date_str']
                    self.end_date_numeric = date_utils.convert_date_fmt(self.end_date)
                    self.logger.warn("TV ETL Processing end_date is being modified to {} for this trigger".format(self.end_date))
            
        '''Notification/Validation Msg '''
        if available_kantar_weeks is None:
            self.kantar_validation_dict['start_week'] = 'NA'
            self.kantar_validation_dict['end_week'] = 'NA'
            self.kantar_validation_dict['start_date'] = 'NA'
            self.kantar_validation_dict['end_date'] = 'NA'
            self.kantar_validation_dict['description'] = 'Insuffiecient data for kantar in given time interval.'
            self.errored = True
            self.errored_msgs_list.append("Insuffiecient data for Kantar in given time interval.")
            self.logger.warn(self.kantar_validation_dict['description'])

        elif kantar_end_year_week['week'] > start_week_flag - 1:
            self.kantar_validation_dict['start_week'] = str(self.kantar_start_week)
            self.kantar_validation_dict['end_week'] = str(self.kantar_end_week)
            self.kantar_validation_dict['start_date'] = self.start_date
            self.kantar_validation_dict['end_date'] = self.end_date
            if self.kantar_year == kantar_end_year_week['year']:
                self.kantar_validation_dict['description'] = 'Kantar data is not available for kantar week {} \
                Therefore, ETL proccessing modified till kantar week {} of year {}. \
                '.format(kantar_end_year_week['week'], str(self.kantar_end_week), str(self.kantar_year))
            else:
                self.kantar_validation_dict['description'] = 'Due to year transition \
                Therefore, ETL proccessing modified till kantar week {} of year({}). \
                '.format(str(self.kantar_end_week), str(self.kantar_year))

        else:
            self.kantar_validation_dict['start_week'] = str(self.kantar_start_week)
            self.kantar_validation_dict['end_week'] = str(self.kantar_end_week)
            self.kantar_validation_dict['start_date'] = self.start_date
            self.kantar_validation_dict['end_date'] = self.end_date
            self.kantar_validation_dict['description'] = 'Kantar data is available for given date range.'
            self.logger.info(self.kantar_validation_dict['description'])



#             self.kantar_end_week = start_week_flag - 1
            
#             kantar_available_end_date = date_utils.get_date_range_for_kantar_week(self.kantar_end_week,
#                                         self.kantar_year)['week_end_date_str']

#             kantar_available_end_date_numeric = date_utils.convert_date_fmt(kantar_available_end_date)
#             self.end_date_numeric = date_utils.convert_date_fmt(end_date)
#             if self.end_date_numeric > kantar_available_end_date_numeric:
#                 self.end_date_numeric = kantar_available_end_date_numeric
#                 self.end_date = kantar_available_end_date


            #---------------------------
            # TO Notify whether kantar out of order delivery
            #------------------------------ 
            # max_week = self.__extract_val_from_hive(query=Extract_HQL_Const.max_kantar_week_exist,
            #                             extracted_key = 'week', 
            #                             db = self.db, 
            #                             table=TableName.kantar_log,
            #                             year = str(self.kantar_year))
            # if max_week is None:
            #     self.logger.error('System is not able to recognize Kantar weekly log')
            #     assert False, 'System is not able to recognize Kantar weekly log'

            # if max_week < self.kantar_end_week:
            #     self.logger.warn("Insuffiecient data for Kantar for week {}".format(self.kantar_end_week))
            #     max_kantar_end_date = date_utils.get_date_range_for_kantar_week(max_week, 
            #                             self.kantar_year)['week_end_date_str']
            #     max_kantar_end_date_numeric = date_utils.convert_date_fmt(max_kantar_end_date)
            #     if int(max_kantar_end_date_numeric) >= self.start_date_numeric:
            #         self.end_date = max_kantar_end_date
            #     else:
            #         assert False, 'Insuffiecient data for Kantar'

        
        
        ''' Experian data availablity validations'''
        most_recent_experian = 21000101
        last_end_date = self.end_date
        required_exp_data_for_day = self.end_date
        if not mock_run:
            most_recent_experian = self.__extract_val_from_hive(query=Extract_HQL_Const.max_experian_data_exist,
                                        extracted_key = 'file_name', 
                                        db = self.db, 
                                        table=TableName.experian_mango)
            max_exper_tivo_sync_considered = 7
            required_exp_data_for_day = date_utils.date_operations(self.end_date, days=max_exper_tivo_sync_considered)
            required_exp_data_for_day_numeric = date_utils.convert_date_fmt(required_exp_data_for_day)
            if most_recent_experian < int(required_exp_data_for_day_numeric):
                self.logger.warn("Insuffiecient data in Experian Mango for end_date {} to process".format(self.end_date))
                self.end_date = date_utils.convert_date_fmt(str(most_recent_experian), current_fmt = '%Y%m%d', output_fmt = '%Y-%m-%d')
                self.logger.warn("Last Experian Mango data is available for date{}".format(self.end_date))
                self.logger.warn("Experian Mango end_date is being modified to {} for this trigger".format(self.end_date))
                #assert False, 'Insuffiecient data for Experian'
        
        self.start_date_numeric = date_utils.convert_date_fmt(self.start_date)
        if int(self.start_date_numeric) <= most_recent_experian:
            if last_end_date != self.end_date:
                self.experian_validation_dict['start_date'] = self.start_date
                self.experian_validation_dict['end_date'] = self.end_date
                self.experian_validation_dict['description'] = 'Experian data is not available for day {} which is needed \
                        Hence, ETL proccessing modified till  {}. \
                        '.format(required_exp_data_for_day, self.end_date)
            else:
                self.experian_validation_dict['start_date'] = self.start_date
                self.experian_validation_dict['end_date'] = self.end_date
                self.experian_validation_dict['description'] = 'Experian data is available for day {} \
                        Hence, ETL can be proccessed till  {}. \
                        '.format(required_exp_data_for_day, self.end_date)
                self.logger.info(self.experian_validation_dict['description'])
        else:
            self.experian_validation_dict['start_date'] = 'NA'
            self.experian_validation_dict['end_date'] = 'NA'
            self.experian_validation_dict['description'] = 'Insuffiecient data for Experian in required time interval.'
            self.errored = True
            self.errored_msgs_list.append("Insuffiecient data for Experian in required time interval.")
            self.logger.warn(self.experian_validation_dict['description'])

        self.start_date_numeric = date_utils.convert_date_fmt(self.start_date)
        self.end_date_numeric = date_utils.convert_date_fmt(self.end_date)
        exp_start_date = date_utils.date_operations(self.start_date, days=experian_lookback)
        self.exp_start_date_numeric = date_utils.convert_date_fmt(exp_start_date)
        self.exp_end_date_numeric = self.end_date_numeric

        self.logger.info("\n")
        self.logger.info("Extracted data availablity info in which ETL processing will be procced.\n")
        self.logger.info("    Final Start date :  {}".format(self.start_date))
        self.logger.info("    Final End date   :  {}\n".format(self.end_date))
        
       
    def __extract_val_from_hive(self, query, extracted_key, **kwargs):
        df = SQL.run_query(query=query.format(**kwargs), return_data=True)
        if df is not None and df[extracted_key] is not None \
            and len(df[extracted_key]) >= 1 and str(df[extracted_key][0]).isnumeric():
            return df[extracted_key][0]
        else : 
            return None

    def __extract_sorted_list_from_hive(self, query, extracted_key, **kwargs):
        data = self.__extract_list_from_hive(query, extracted_key, **kwargs)
        if data is not None: 
            data.sort()
            return data
        return None

    def __extract_list_from_hive(self, query, extracted_key, **kwargs):
        data = self.__extract_col_dict_from_hive(query, [extracted_key], **kwargs)
        if data is not None:
            return data[extracted_key]
        return None


    def __extract_col_dict_from_hive(self, query, extracted_keys: list, **kwargs):
        df = SQL.run_query(query=query.format(**kwargs), return_data=True)
        if df is not None:
            result = {}
            for col in extracted_keys:
                data = df[col]
                if data is not None and len(data) > 0:
                    data_list = list(data)
                    result[col] = data_list
                else:
                    result[col] = None
            return result        
        return None

    def create_setup_script(self):
        db=self.db
        location=self.location
        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.hive_settings) \
                   .append(HQL_Const.create_db, db=db, location=location) \
                   .append(HQL_Const.create_tivo_ext_table, db=db, 
                        table=TableName.tivo_log, 
                        location=S3Location.tivo_s3_incremental_data) \
                   .append(HQL_Const.create_kantar_log_table, db=db, 
                        table=TableName.kantar_log) \
                   .append(HQL_Const.create_creative_ext_table, db=db, 
                        table=TableName.creative) \
                   .append(HQL_Const.create_ext_experian_crosswalk, db=db, 
                        table=TableName.experian_crosswalk,
                        location=S3Location.exparian_crosswalk_s3) \
                   .append(HQL_Const.create_ext_experian_mango, db=db, 
                        table=TableName.experian_mango,
                        location=S3Location.exparian_mango_s3) \
                   .append(HQL_Const.create_ext_market_table, db=db, 
                        table=TableName.market,
                        location=S3Location.market) \
                   .append(HQL_Const.create_ext_oproduct_table, db=db, 
                        table=TableName.oproduct,
                        location=S3Location.oproduct) \
                   .append(HQL_Const.create_ext_product_table, db=db, 
                        table=TableName.product,
                        location=S3Location.product) \
                   .append(HQL_Const.create_ext_media_table, db=db, 
                        table=TableName.media,
                        location=S3Location.media) \
                   .append(HQL_Const.create_ext_product_name_table, db=db, 
                        table=TableName.product_name,
                        location=S3Location.product_name) \
                   .append(HQL_Const.create_ext_property_table, db=db, 
                        table=TableName.property,
                        location=S3Location.property) \
                   .append(HQL_Const.create_ext_advertiser_table, db=db, 
                        table=TableName.advertiser,
                        location=S3Location.advertiser) \
                   .append(HQL_Const.create_ext_ultimateowner_table, db=db, 
                        table=TableName.ultimateowner,
                        location=S3Location.ultimateowner) \
                   .append(HQL_Const.create_ext_oprog_table, db=db, 
                        table=TableName.oprog,
                        location=S3Location.oprog) \
                   .append(HQL_Const.create_ext_prog_table, db=db, 
                        table=TableName.prog,
                        location=S3Location.prog) \
                   .append(HQL_Const.create_dma_mapping_table, db=db, 
                        table=TableName.dma_mapping,
                        location=S3Location.kantar_tivo_dma_mapping) \
                   .append(HQL_Const.create_channel_mapping_table, db=db, 
                        table=TableName.channel_mapping,
                        location=S3Location.kantar_tivo_channel_mapping) \
                   .append(HQL_Const.create_mta_tv_impressions_table, db=db, 
                        table=TableName.mta_tv_impressions,
                        location=S3Location.mta_impressions_result_loc) \
                   .append(HQL_Const.create_kantar_creative_lmt_table, db=db, 
                        table=TableName.creative_lmt,
                        location=S3Location.creative_lmt_result_loc) \
                   .append(HQL_Const.create_creative_stat_table, db=db, 
                        table=TableName.stat_creative_lmt,
                        location=S3Location.stat_creative) \
                   .append(HQL_Const.create_impression_stat_table, db=db, 
                        table=TableName.stat_mta_tv_impressions,
                        location=S3Location.stat_mta_impressions) \
                   .append(HQL_Const.create_hosted_ultimateowner_id_table, db=db, 
                        table=TableName.hosted_ultimateowner_id,
                        location=S3Location.hosted_ultimateowner_ids) \
                   .append(HQL_Const.ext_table_repair, db=db, table=TableName.tivo_log) \
                   .append(HQL_Const.ext_table_repair, db=db, table=TableName.experian_mango) \
                   .append(HQL_Const.ext_table_repair, db=db, table=TableName.mta_tv_impressions) \


        create_schema_script = hql_builder.value()
        self.logger.debug("\n\n Create Schema Script: \n\n"+create_schema_script)
        return create_schema_script
