import tv.py_path
import tv.path_resolver
import os, sys
import re
import time
import logging
import traceback
import io
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

from tv.services.extractor import Extractor
from tv.services.ds_sync import DBMetadataSync
from tv.services.transformer import Transformer
from tv.services.creative_lmt import CreativeLmt
from tv.services.load import Load
from tv.services.util_lib import Hive_Common_Util

class TVETL:

    def __init__(self, started_on=int(time.time())):
        self.logger = logging.getLogger(__name__)
        self.global_started_on = started_on
        self.fail_nofification_data = {}
        self.fail_nofification_data['trigger_on'] = started_on
        self.fail_nofification_data['status'] = 'FAILED'
        self.fail_nofification_data['staging_path'] = Config.staging_loc_path()


    def set_up(self, start_date, started_on=int(time.time()), schema_change=True, dry_run=True, mock_run = True):
        try:
            self.__set_up(start_date, started_on=started_on,
                          schema_change=schema_change, dry_run=dry_run, mock_run = mock_run)
            kantar_start_year_week = date_utils.get_kantar_week_with_year(start_date, fmt = '%Y-%m-%d')
            kantar_start_year = kantar_start_year_week['year']
            kantar_start_week = kantar_start_year_week['week']
            creativeLmt : CreativeLmt = CreativeLmt()
            creativeLmt.setup_creative_lmt(kantar_start_year, kantar_start_week, started_on, dry_run=dry_run, mock_run=mock_run )

        except Exception as e:
            logging.info("\n==========TV ETL Setup has failed!!=================\n")
            completed_on = int(time.time())
            StateService.status = 'FAILED'
            StateService.desc = "Tv etl setup has failed"
            StateService.finished_on = completed_on
            StateService.stat_persist(started_on, dry_run=dry_run, mock_run=mock_run)
            started_on_date = datetime.fromtimestamp(started_on)
            completed_on_date = datetime.fromtimestamp(completed_on)
            logging.info("Time taken by TV ETL Setup: {}  [#days days, hh:mm:ss]" \
                         .format(str(completed_on_date-started_on_date)))
            traceback.print_exc(file=sys.stdout)
            logging.error(e, exc_info=True)
            traceback.print_stack()

    def manual_etl(self, start_date, end_date, result_loc=None,  ultimate_owner_id_list=None, 
                filter=None, started_on=int(time.time()), dry_run=True, mock_run = True):
        config_data = Config.get_configs()
        if config_data is not None:
            if config_data.get('trigger_no_of_days') is not None:
                trigger_no_of_days = int(config_data.get('trigger_no_of_days'))
            if config_data.get('max_recursive_run') is not None:
                max_recur_run = int(config_data.get('max_recursive_run'))
        self.logger.info(f"Trigger for #days (trigger_no_of_days):{trigger_no_of_days}")
        self.logger.info(f"Max Recursive Run (max_recur_run):{max_recur_run}")
        self.__manual_impression_trigger(start_date = start_date, end_date = end_date,
                               result_loc=result_loc,
                               trigger_no_of_days = trigger_no_of_days,
                               ultimate_owner_id_list=ultimate_owner_id_list,
                               filter=filter,
                               started_on=started_on,
                               max_recur_run=max_recur_run,
                               dry_run=dry_run,
                               mock_run = mock_run)

    
    def default_etl(self, started_on=int(time.time()), dry_run=True, mock_run = True):
        self.default_impression_trigger(started_on=started_on,
                               dry_run=dry_run,
                               mock_run = mock_run)
        
        self.update_creative_lmt(started_on= started_on, dry_run=dry_run, mock_run = mock_run)
        NotificationService.notify_from_queue("Default_Trigger", dry_run, mock_run)
        if not dry_run and not mock_run:
            self.copy_kantar_lmts()

    
    def default_impression_trigger(self, trigger_no_of_days = 7, started_on=int(time.time()), max_recur_run = 5, dry_run=True, mock_run = True):
        try:
            config_data = Config.get_configs()
            if config_data is not None:
                if config_data.get('trigger_no_of_days') is not None:
                    trigger_no_of_days = int(config_data.get('trigger_no_of_days'))
                if config_data.get('max_recursive_run') is not None:
                    max_recur_run = int(config_data.get('max_recursive_run'))
            self.logger.info(f"Trigger for #days (trigger_no_of_days):{trigger_no_of_days}")
            self.logger.info(f"Max Recursive Run (max_recur_run):{max_recur_run}")
            self.__default_impression_trigger(trigger_no_of_days = trigger_no_of_days,
                               started_on=started_on,
                               max_recur_run=max_recur_run,
                               dry_run=dry_run,
                               mock_run = mock_run)

        except Exception as e:
            logging.info("\n==========Default TV ETL run has failed!!=================\n")
            completed_on = int(time.time())
            self.fail_nofification_data['description'] = 'Default Tv etl run has failed!!'
            self.fail_nofification_data['error_type'] = 'MTA_IMP_GENERATION_FAILED'
            self.fail_nofification_data['stack_trace'] = self.get_stack_exc_str()
            NotificationService.notify(self.fail_nofification_data, dry_run, mock_run)
            StateService.status = 'FAILED'
            StateService.desc = "Default Tv etl run has failed"
            StateService.finished_on = completed_on
            self.logger.debug("Failed Stat persisting at time stamp: {}".format(str(StateService.started_on)))
            StateService.stat_persist(StateService.started_on, dry_run=dry_run, mock_run=mock_run)
            started_on_date = datetime.fromtimestamp(started_on)
            completed_on_date = datetime.fromtimestamp(completed_on)
            logging.info("Time taken by Default TV ETL run: {}  [#days days, hh:mm:ss]" \
                         .format(str(completed_on_date-started_on_date)))
            
            traceback.print_exc(file=sys.stdout)
            logging.error(e, exc_info=True)
            traceback.print_stack()
            
            # TODO If 1st recursive trigger get fails
            item = NotificationService.get("Default_Trigger")
            if item == None:
                #NotificationService.put_in_queue("Default_Trigger", {"SkipCreativeUpdate" : True})
                raise Exception("The first recursive trigger it self failed so stopped rest of processing.")
            ####


    def update_creative_lmt(self, started_on=int(time.time()), dry_run=True, mock_run = True):
        try:
            started_on_date = datetime.fromtimestamp(int(time.time()))
            if not dry_run:
                StateService.stat_sync(dry_run=dry_run, mock_run=mock_run)
            impressions_end_date = StateService.last_completed_run
            kantar_end_year_week = date_utils.get_kantar_week_with_year(impressions_end_date, fmt = '%Y-%m-%d')
            kantar_end_year = kantar_end_year_week['year']
            kantar_end_week = kantar_end_year_week['week']

            creativeLmt : CreativeLmt = CreativeLmt()
            
            self.logger.info("Creative LMT Update has started at {}.".format(started_on_date))
            creativeLmt.update_creative_lmt(kantar_end_year, kantar_end_week, started_on, dry_run, mock_run)
            completed_on=int(time.time())
            completed_on_date = datetime.fromtimestamp(completed_on)
            self.logger.info("Creative LMT Update  has completed at {}.".format(completed_on_date))
            logging.info("Time taken by Creative LMT Update : {}  [#days days, hh:mm:ss]".format(str(completed_on_date-started_on_date)))
            self.logger.info("Creative LMT Update has Done Successfully")
            
            item = NotificationService.get("Default_Trigger")
            item['creative_lmt'] = creativeLmt.creative_validation_data()
        except Exception as e:
            logging.info("\n==========Update Creative LMT run has failed!!=================\n")
            self.fail_nofification_data['description'] = 'Update Creative LMT run has failed!!'
            self.fail_nofification_data['error_type'] = 'CREATIVE_UPDATE_FAILED'
            self.fail_nofification_data['start_week'] = creativeLmt.start_week()
            self.fail_nofification_data['start_year'] = creativeLmt.start_year()
            self.fail_nofification_data['end_week'] = kantar_end_week
            self.fail_nofification_data['end_year'] = kantar_end_year
            self.fail_nofification_data['stack_trace'] = self.get_stack_exc_str()
            NotificationService.notify(self.fail_nofification_data, dry_run, mock_run)
            traceback.print_exc(file=sys.stdout)
            logging.error(e, exc_info=True)
            traceback.print_stack()
            raise e


    def __set_up(self, start_date, started_on=int(time.time()), schema_change=True, dry_run=True, mock_run = True):

        log_util.print_log_banner(" TV ETL SET UP")
        started_on_date = datetime.fromtimestamp(started_on)
        self.logger.info("TV ETL Setup has started at {}.".format(started_on_date))
        StateService.setup(start_date, started_on=started_on, dry_run=dry_run, mock_run=mock_run)

        if not schema_change:
            return
        e : Extractor = Extractor()
        setup_script = e.create_setup_script()

        self.logger.info("==================================================")
        self.logger.info("Setup Script: \n\n"+setup_script)
        self.logger.info("==================================================")
        if not dry_run and not mock_run:
            self.logger.debug("\n\n Set up script is executing: \n\n")
            SQL.run_query(query=setup_script, raw=True, return_data=False, execution_engine='HIVE')
        completed_on = int(time.time())
        completed_on_date = datetime.fromtimestamp(completed_on)
        self.logger.info("TV ETL Setup has completed at {}.".format(completed_on_date))
        logging.info("Time taken by TV ETL Setup: {}  [#days days, hh:mm:ss]".format(str(completed_on_date-started_on_date)))
        self.logger.info("TV ETL Setup is Done Successfully")


    
    def __default_impression_trigger(self, trigger_no_of_days = 7, started_on=int(time.time()), 
                max_recur_run=5,  using_internal_views=False, dry_run=True, mock_run = True):
    
        log_util.print_log_banner("DEFAULT TV ETL RUN")
        global_started_on_date = datetime.fromtimestamp(started_on)
        self.logger.info("ETL Default Transformation has started at {}.".format(global_started_on_date))

        mta_tv_impressions_tab = Load.create_result_table(S3Location.mta_impressions_result_loc, dest_db=System_Prop.staging_db, dry_run=dry_run, mock_run=mock_run)
        
        for i in range(max_recur_run):
            ''' Default Etl Run  '''
            recur_run_started_on = int(time.time())

            #Recursive Started on collected in fail dict
            self.fail_nofification_data['recur_started_on'] = recur_run_started_on
            self.fail_nofification_data['recursive_trigger_num'] = i + 1

            started_on_date = datetime.fromtimestamp(recur_run_started_on)
            self.logger.info("ETL Default Transformation part {} has started at {}.".format(str(i+1), started_on_date))

            ''' Load Previous run stat.
            '''
            StateService.stat_sync(dry_run=dry_run, mock_run=mock_run)
            StateService.parent_started_on = started_on
            StateService.started_on = recur_run_started_on
            StateService.finished_on = -1

            start_date = date_utils.date_operations(StateService.last_completed_run, operator='+', days=1)
            end_date = date_utils.date_operations(start_date, operator='+', days=trigger_no_of_days -1)
            StateService.tv_start_date = start_date
            StateService.trigger_no_of_days = trigger_no_of_days
            StateService.desc = "Tv etl for start_date: {} and end_date {} has started.".format(start_date,end_date)
            StateService.stat_persist(StateService.started_on, dry_run=dry_run, mock_run=mock_run)
            orig_start_date = start_date
            orig_end_date = end_date

            self.fail_nofification_data['start_date'] = orig_start_date
            self.fail_nofification_data['end_date'] = orig_end_date

            '''
            Metadata Sync.
            '''
            db_metadataSync : DBMetadataSync = DBMetadataSync()
            db_metadataSync.sync(dry_run=dry_run, mock_run=mock_run)

            
            ''' Extracting details from input data sources to confirm 
                TV ETL possible for date ranges.
            '''
            e : Extractor = Extractor()
            e.intialise(start_date, end_date, dry_run=dry_run, mock_run=mock_run)
            start_date = e.start_date
            end_date = e.end_date
            start_date_numeric = e.start_date_numeric
            end_date_numeric = e.end_date_numeric

            exp_start_date = e.exp_start_date_numeric
            exp_end_date = e.exp_end_date_numeric

            start_week = e.kantar_start_week
            end_week = e.kantar_end_week
            year = e.kantar_year

            # Adding Stat description and end date
            StateService.tv_end_date = end_date

            # Validate data
            validation_data = {}
            # NotificationService.put_in_queue("Default_Trigger", validation_data)
            validation_data['start_date'] = orig_start_date
            validation_data['end_date'] = orig_end_date
            validation_data['tivo'] = e.tivo_validation_data()
            validation_data['kantar'] = e.kantar_validation_data()
            validation_data['experian'] = e.experian_validation_data()
            validation_data['errored_msgs'] = e.errored_msgs()
            validation_data['is_errored'] = e.is_errored()
            validation_data['trigger_on'] = started_on
            recursive_trig = {}
            recursive_trig['recursive_trigger_num'] = i+1
            recursive_trig['recur_started_on'] = recur_run_started_on
            #recursive_trig[''] = ''
            validation_data['recursive_trigger']=recursive_trig
            validation_data['tv_start_date']=start_date
            validation_data['tv_end_date']=end_date
            validation_data['status']='SUCCESSFUL'
            validation_data['staging_path'] = Config.staging_loc_path()

            if  e.is_errored() \
                or start_date_numeric > end_date_numeric \
                or exp_start_date > exp_end_date \
                or start_week > end_week:
                self.logger.error("Invalid date ranges. It could due to insufficient data for Processing!!")
                # self.logger.error("Extrating Dates are \
                #     \n {}: {}\n {}: {}\n {}: {}\n {}: {}\n {}: {}\n {}: {}\n {}: {}\n \
                #     ".format("start_date", self.start_date, "end_date", self.end_date, 
                #                "exp_start_date",self.exp_start_date_numeric, "exp_end_date", exp_end_date_numeric,
                #                 "start_week", self.kantar_start_week, "end_week", self.kantar_end_week, "year", self.kantar_year))

                #assert False, 'ETL Processing is not possible for in appropriate date range'
                StateService.finished_on = int(time.time())
                StateService.status = 'SKIPPED'
                StateService.stat_persist(StateService.started_on, dry_run=dry_run, mock_run=mock_run)
                validation_data['status']='SKIPPED'
                NotificationService.put_in_queue("Default_Trigger", validation_data)
                break
            else:
                StateService.last_completed_run = end_date
                StateService.desc = "Tv etl for start_date: {} and end_date {} in progress".format(start_date,end_date)
            StateService.stat_persist(StateService.started_on, dry_run=dry_run, mock_run=mock_run)

            self.fail_nofification_data['tv_start_date'] = start_date
            self.fail_nofification_data['tv_end_date'] = end_date
            
            #TO test fail scenario
            #raise Exception("Testing")

            ''' Transfomation Script Creation.
            '''
            transformer : Transformer =Transformer(start_date =start_date, end_date = end_date,
                    kantar_year = year, start_week = start_week, end_week = end_week, 
                    exp_start_date = exp_start_date, exp_end_date = exp_end_date,    
                    start_timestamp=started_on)

            transform_script = transformer.create_transform_script(using_views=using_internal_views)

            ''' Load result Script Creation.
            '''
            
            #load : Load = Load(src_table = transformer.target_table_name)
            load : Load = Load(src_table = transformer.target_table_name, dest_table = mta_tv_impressions_tab)
            load_script = load.create_load_script()
            # load_script = load.create_load_using_views_script()
            
            etl_query_script = "{}{}".format(transform_script, load_script)
            self.logger.info("==================================================")
            self.logger.info("ETL Default Run: \n\n"+etl_query_script)
            self.logger.info("==================================================")
            if not dry_run and not mock_run:
                self.logger.debug("\n\n ETL Default trigger script is executing: \n\n")
                SQL.run_query(query=etl_query_script, raw=True, return_data=False)

            
            cleanup_script = transformer.cleanup_hive_intermediate_script(is_view=using_internal_views)
            self.logger.info("==================================================")
            self.logger.info("ETL Cleanup Script: \n\n"+cleanup_script)
            self.logger.info("==================================================")
            if not dry_run and not mock_run:
                self.logger.debug("\n\n ETL Cleanup script is executing: \n\n")
                SQL.run_query(query=cleanup_script, raw=True, return_data=False)

            StateService.finished_on = int(time.time())
            StateService.status = 'COMPLETED'
            StateService.desc = "Tv etl for start_date: {} and end_date {} has completed.".format(start_date,end_date)
            StateService.stat_persist(StateService.started_on, dry_run=dry_run, mock_run=mock_run)

            completed_on = StateService.finished_on
            completed_on_date = datetime.fromtimestamp(completed_on)
            self.logger.info("TV ETL Default Transformation part-{} has completed at {}.".format(str(i+1), completed_on_date))
            self.logger.info("Time taken by TV ETL Default Transformation part-{}: {}  [#days days, hh:mm:ss]".format(str(i+1), str(completed_on_date-started_on_date)))
            # TO Isolate states for each run during dry_run
            if max_recur_run > i + 1:
                validation_data['creative_lmt'] = {}
                validation_data['creative_lmt']['msg']= 'There is no Creative LMT update run as part of this recursive run.'
                NotificationService.notify(validation_data, dry_run, mock_run)
           
            ###############################################
            ###############################################
            #TO test fail scenario (Notification service testing)
            
            #      Testing case-1
            # if max_recur_run == i+1:
            #     raise Exception("Testing")
            
            #     Testing case-2 
            # (when max_recur_run greater than 4 and Testing case-3 should be commented  )
            # if i == 2:
            #     raise Exception("Testing")

            #     Testing case-3
            # if i == 0:
            #     raise Exception("Testing")

            ###############################################
            ###############################################

            #Put Notification data in queue
            NotificationService.put_in_queue("Default_Trigger", validation_data)
            
            time.sleep(2)

        # #Put Notification data in queue
        # NotificationService.put_in_queue("Default_Trigger", validation_data)
        
        Hive_Common_Util.drop_table(db=System_Prop.staging_db,  table=mta_tv_impressions_tab, dry_run=dry_run, mock_run=mock_run)
        completed_on_date = datetime.fromtimestamp(int(time.time()))
        self.logger.info("TV ETL Default Transformation has completed at {}.".format(completed_on_date))
        logging.info("Time taken by TV ETL Default Transformation: {}  [#days days, hh:mm:ss]".format(str(completed_on_date-global_started_on_date)))   
        self.logger.info("TV ETL Default Transformation has Done Successfully\n\n\n\n")
        self.logger.info("===============================================================")

    
    def __manual_impression_trigger(self, start_date, end_date, result_loc = None, 
            ultimate_owner_id_list=None, filter=None, trigger_no_of_days = 7, 
            started_on=int(time.time()), max_recur_run=5, using_internal_views=False, 
            skip_partitioning=False, dry_run=True, mock_run = True):
        
        if ultimate_owner_id_list is not None:
            max_recur_run = 1
            if end_date is None:
                trigger_no_of_days = 60
            else:
                days = date_utils.date_diff(end_date, start_date)                
                trigger_no_of_days = 30
                max_recur_run = int(days/trigger_no_of_days + 1)

        log_util.print_log_banner("MANUAL TV ETL RUN")
        global_started_on_date = datetime.fromtimestamp(started_on)
        self.logger.info("ETL Manual Transformation has started at {}.".format(global_started_on_date))

        end_date_orig = end_date
        end_date = date_utils.date_operations(start_date, operator='+', days=trigger_no_of_days -1)
        
        if end_date_orig is None:
            end_date_orig = end_date
        end_date_orig_numeric = date_utils.convert_date_fmt(end_date_orig)

        if skip_partitioning:
            max_recur_run = 1
            trigger_no_of_days = date_utils.date_diff(end_date_orig, start_date)
            end_date = end_date_orig
        
        if result_loc is None:
            result_loc = S3Location.mta_impressions_result_loc
        mta_tv_impressions_tab = Load.create_result_table(result_loc, dest_db=System_Prop.staging_db, dry_run=dry_run, mock_run=mock_run)
        
        for i in range(max_recur_run):
            ''' Manual Etl Run  '''
            recur_run_started_on = int(time.time())
            started_on_date = datetime.fromtimestamp(recur_run_started_on)
            self.logger.info("ETL Manual Transformation part {} has started at {}.".format(str(i+1), started_on_date))
            
            end_date_numeric = date_utils.convert_date_fmt(end_date)
            if int(end_date_orig_numeric) < int(end_date_numeric):
                end_date = end_date_orig


            '''
            Metadata Sync.
            '''
            db_metadataSync : DBMetadataSync = DBMetadataSync()
            db_metadataSync.sync(dry_run=dry_run, mock_run=mock_run)

            
            ''' Extracting details from input data sources to confirm 
                TV ETL possible for date ranges.
            '''
            e : Extractor = Extractor()
            e.intialise(start_date, end_date, dry_run=dry_run, mock_run=mock_run)
            start_date = e.start_date
            end_date = e.end_date
            start_date_numeric = e.start_date_numeric
            end_date_numeric = e.end_date_numeric

            exp_start_date = e.exp_start_date_numeric
            exp_end_date = e.exp_end_date_numeric

            start_week = e.kantar_start_week
            end_week = e.kantar_end_week
            year = e.kantar_year

            if  e.is_errored() \
                or start_date_numeric > end_date_numeric \
                or exp_start_date > exp_end_date \
                or start_week > end_week:
                
                self.logger.error("Invalid date ranges. It could due to insufficient data for Processing!!")
                break


            ''' Transfomation Script Creation.
            '''
            transformer : Transformer =Transformer(start_date =start_date, end_date = end_date,
                    kantar_year = year, start_week = start_week, end_week = end_week, 
                    exp_start_date = exp_start_date, exp_end_date = exp_end_date,    
                    start_timestamp=started_on)

            transform_script = transformer.create_transform_script(ultimate_owner_id_list=ultimate_owner_id_list, 
                                filter=filter, using_views=using_internal_views)
        
            self.logger.info("==================================================")
            self.logger.info("ETL Transform Script: \n\n"+transform_script)
            self.logger.info("==================================================")
            if not dry_run and not mock_run:
                self.logger.debug("\n\n ETL Transform script is executing: \n\n")
                SQL.run_query(query=transform_script, raw=True, return_data=False)

            ''' Load result Script Creation.
            '''
            load : Load = Load(src_table = transformer.target_table_name, dest_table = mta_tv_impressions_tab)
            load_script = load.create_load_script()
            self.logger.info("==================================================")
            self.logger.info("ETL Load Transform: \n\n"+load_script)
            self.logger.info("==================================================")
            if not dry_run and not mock_run:
                self.logger.debug("\n\n ETL Load Transform trigger script is executing: \n\n")
                SQL.run_query(query=load_script, raw=True, return_data=False)

            cleanup_script = transformer.cleanup_hive_intermediate_script(is_view=using_internal_views)
            self.logger.info("==================================================")
            self.logger.info("ETL Cleanup Script: \n\n"+cleanup_script)
            self.logger.info("==================================================")
            if not dry_run and not mock_run:
                self.logger.debug("\n\n ETL Cleanup script is executing: \n\n")
                SQL.run_query(query=cleanup_script, raw=True, return_data=False)
            
            start_date = date_utils.date_operations(start_date, operator='+', days=trigger_no_of_days)
            end_date = date_utils.date_operations(start_date, operator='+', days=trigger_no_of_days -1 )
            start_date_numeric = date_utils.convert_date_fmt(start_date)
            
            started_on_date = datetime.fromtimestamp(recur_run_started_on)
            completed_on_date = datetime.fromtimestamp(int(time.time()))
            self.logger.info("TV ETL Manual Transformation part-{} has completed at {}.".format(str(i+1), completed_on_date))
            self.logger.info("Time taken by TV ETL Manual Transformation part-{}: {}  [#days days, hh:mm:ss]".format(str(i+1), str(completed_on_date-started_on_date)))
            
            if int(end_date_orig_numeric) < int(start_date_numeric):
                break;
            
            time.sleep(2)
            

        Hive_Common_Util.drop_table(db=System_Prop.staging_db,  table=mta_tv_impressions_tab, dry_run=dry_run, mock_run=mock_run)
        completed_on_date = datetime.fromtimestamp(int(time.time()))
        self.logger.info("TV ETL Manual Transformation has completed at {}.".format(completed_on_date))
        logging.info("Time taken by TV ETL Manual Transformation: {}  [#days days, hh:mm:ss]".format(str(completed_on_date-global_started_on_date)))   
        self.logger.info("TV ETL Manual Transformation has Done Successfully\n\n\n\n")
        self.logger.info("===============================================================")
    
    
    def get_stack_exc_str(self):
        f = io.StringIO()
        traceback.print_exc(file=f)
        stack_str = f.getvalue()
        return stack_str.replace('\n','<br>')

    def copy_kantar_lmts(self):
        try:
            kantar_master_src_loc = S3Location.kantar_master_dir
            kantar_tv_master_src_loc = S3Location.kantar_tv_master  
            kantar_master_dest_loc = S3Location.kantar_master_lmts_dir
            kantar_tv_master_dest_loc = S3Location.kantar_tv_master_lmts_dir
            S3.copy(kantar_master_src_loc, kantar_master_dest_loc, op_type = 'TRANSFER', recursive=True)
            S3.copy(kantar_tv_master_src_loc, kantar_tv_master_dest_loc, op_type = 'TRANSFER', recursive=True)
        except Exception as e:
            self.logger.error("Kantar LMTs Copying Failed")
            traceback.print_exc(file=sys.stdout)
            logging.error(e, exc_info=True)
            traceback.print_stack()
            


# if __name__ == '__main__':
#    print('TVETL')
