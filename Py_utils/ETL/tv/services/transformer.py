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
from tv.spark_transform_script2 import Transform_Spark_Const
from tv.part_hive_query import Partial_HQL_Const
from tv.extract_script import Extract_HQL_Const

#from tv.util.s3_utils import S3
from tv.util.neucmd.neuhadoop import S3

from tv.util import date_utils
from tv.util import log_util
from tv.states import StateService
from tv.stats.stats import StatUtil
from tv.util.notification_service import NotificationService


class Transformer:
    
    '''
        start_date and end_date should be in format yyyy-mm-dd
    '''
    def __init__(self, start_date: str, end_date: str, 
                        kantar_year:int, start_week:int, end_week:int, 
                        exp_start_date:int, exp_end_date:int,
                        start_timestamp=int(time.time())):

        self.logger = logging.getLogger(__name__)

        start_date_int = date_utils.convert_date_fmt(start_date)
        end_date_int = date_utils.convert_date_fmt(end_date)

        if start_date_int == end_date_int: self.duration = start_date_int
        else: self.duration = '{}_to_{}'.format(start_date_int, end_date_int)
        self.start_timestamp = start_timestamp
        self.start_timestamp_str = str(start_timestamp)
        self.start_date = date_utils.valid_date_fmt(start_date)
        self.end_date = date_utils.valid_date_fmt(end_date)
        
        self.result_table = TableName.tv_impressions.format(self.duration, self.start_timestamp_str)
        

        self.exp_start_date_numeric = exp_start_date
        self.exp_end_date_numeric = exp_end_date
        
        self.kantar_year = kantar_year
        self.kantar_start_week = start_week
        self.kantar_end_week = end_week

    @property
    def target_table_name(self) -> str:
        return self.result_table

    def create_transform_script(self, ultimate_owner_id_list=None,
                               filter='LIMITED_ULTIMATEOWNER_ID', using_views=True):
        if Config.execution_engine =='SPARK' :
            return self.create_spark_transform_tables_script(ultimate_owner_id_list=ultimate_owner_id_list, filter=filter)
        if using_views:
            return self.create_transform_view_script(ultimate_owner_id_list=ultimate_owner_id_list, filter=filter)
        
        return self.create_transform_tables_script(ultimate_owner_id_list=ultimate_owner_id_list, filter=filter)


    def create_transform_tables_script(self, ultimate_owner_id_list=None,
                               filter='LIMITED_ULTIMATEOWNER_ID'):

        db=System_Prop.staging_db
        #location=System_Prop.staging_s3_loc
        start_timestamp_str = self.start_timestamp_str
        tb_suffix = '{}_{}'.format(self.duration, self.start_timestamp_str)


        ''' Tables names for transform '''
        self.tivo_resolved = TableName.tivo_resolved.format(tb_suffix)
        self.kantar_resolved = TableName.kantar_resolved.format(tb_suffix)
        self.experian_resolved = TableName.experian_resolved.format(tb_suffix)
        self.tivo_kantar_merged = TableName.tivo_kantar_merged.format(tb_suffix)
        
        tv_impressions = TableName.tv_impressions.format(self.duration, start_timestamp_str)

        ''' Internal variables for tivo in scripts to resolve '''
        raw_tivo = TableName.tivo_log
        start_date = self.start_date
        end_date = self.end_date
        start_date_int = date_utils.convert_date_fmt(start_date)
        end_date_int = date_utils.convert_date_fmt(end_date)
        kantar_tivo_dma_mapping = TableName.dma_mapping
        kantar_tivo_channel_mapping = TableName.channel_mapping
        # kantar_tivo_channel_mapping_mz = TableName.channel_mapping_mz
        # kantar_tivo_channel_mapping_basic = TableName.channel_mapping_basic
        

        ''' Internal variables for Kantar in scripts to resolve '''
        raw_kantar = TableName.kantar_log
        raw_creative = TableName.creative
        year = str(self.kantar_year)
        start_week = str(self.kantar_start_week)
        end_week = str(self.kantar_end_week)
        raw_market = TableName.market
        raw_oproduct = TableName.oproduct
        raw_product = TableName.product
        raw_product_name = TableName.product_name
        raw_property = TableName.property
        raw_media = TableName.media
        raw_advertiser = TableName.advertiser
        raw_oprog = TableName.oprog
        raw_prog = TableName.prog

        if ultimate_owner_id_list is None:
            if filter == 'ALL_ULTIMATEOWNER_ID':
                ultimateowner_id_join_filter=''
            elif filter == 'LIMITED_ULTIMATEOWNER_ID':
                ultimateowner_id_join_filter=Partial_HQL_Const.ultimateowner_id_filteration_join.format(
                    db=db,
                    hosted_ultimateowner_id=TableName.hosted_ultimateowner_id)
            else:
                ultimateowner_id_join_filter=Partial_HQL_Const.ultimateowner_id_filteration_join(
                    db=db,
                    hosted_ultimateowner_id=TableName.hosted_ultimateowner_id)
        else:
            ultmateowner_id_list_str=', '.join('\'{0}\''.format(w) for w in ultimate_owner_id_list)
            self.logger.info("ultmateowner_id_list_str: "+ultmateowner_id_list_str)
            ultimateowner_id_join_filter=Partial_HQL_Const.ultimateowner_id_list_filteration_join.format(ultmateowner_id_list_str=ultmateowner_id_list_str)


        ''' Internal variables for experian in scripts to resolve '''
        experian_mango = TableName.experian_mango
        experian_crosswalk = TableName.experian_crosswalk
        exp_start_date_int = str(self.exp_start_date_numeric)
        exp_end_date_int = str(self.exp_end_date_numeric)

        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.hive_settings) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.tivo_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.kantar_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.experian_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.tivo_kantar_merged) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=tv_impressions) \
                .append(Transform_HQL_Const.create_tivo_resolved_table, db=db, 
                        table=self.tivo_resolved,
                        raw_tivo = raw_tivo,
                        kantar_tivo_dma_mapping = kantar_tivo_dma_mapping,
                        kantar_tivo_channel_mapping = kantar_tivo_channel_mapping,
                        # kantar_tivo_channel_mapping_mz = kantar_tivo_channel_mapping_mz,
                        # kantar_tivo_channel_mapping_basic = kantar_tivo_channel_mapping_basic,
                        start_date_int = start_date_int,
                        end_date_int = end_date_int,
                        start_date = start_date,
                        end_date = end_date) \
                .append(Transform_HQL_Const.create_kantar_resolved_table, db=db, 
                        table=self.kantar_resolved,
                        raw_kantar = raw_kantar,
                        raw_creative = raw_creative,
                        raw_market = raw_market,
                        raw_oproduct = raw_oproduct,
                        raw_product = raw_product,
                        raw_product_name = raw_product_name,
                        raw_property = raw_property,
                        raw_media = raw_media,
                        raw_advertiser = raw_advertiser,
                        raw_oprog = raw_oprog,
                        raw_prog = raw_prog,
                        ultimateowner_id_join_filter=ultimateowner_id_join_filter,
                        year = year,
                        start_week = start_week,
                        end_week = end_week,
                        start_date = start_date,
                        end_date = end_date) \
                .append(Transform_HQL_Const.create_experian_resolved_table, db=db, 
                        table=self.experian_resolved,
                        experian_mango = experian_mango,
                        experian_crosswalk = experian_crosswalk,
                        start_date_int = exp_start_date_int,
                        end_date_int = exp_end_date_int) \
                .append(Transform_HQL_Const.create_tivo_kantar_merged_table, db=db, 
                        table=self.tivo_kantar_merged,
                        tivo_resolved=self.tivo_resolved,
                        kantar_resolved=self.kantar_resolved) \
                .append(Transform_HQL_Const.create_tv_impressions_table, db=db, 
                        table=tv_impressions,
                        tivo_kantar_merged=self.tivo_kantar_merged,
                        experian_resolved=self.experian_resolved,
                        start_date = start_date,
                        end_date = end_date) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.tivo_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.kantar_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.experian_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.tivo_kantar_merged) \

        transform_script = hql_builder.value()
        self.logger.debug("\n\n Transform Script: \n\n"+transform_script)
        return transform_script


    def create_transform_view_script(self, ultimate_owner_id_list=None,
                               filter='LIMITED_ULTIMATEOWNER_ID'):

        db=System_Prop.staging_db
        #location=System_Prop.staging_s3_loc
        start_timestamp_str = self.start_timestamp_str
        tb_suffix = 'view_{}_{}'.format(self.duration, self.start_timestamp_str)


        ''' Tables names for transform '''
        self.tivo_resolved = TableName.tivo_resolved.format(tb_suffix)
        self.kantar_resolved = TableName.kantar_resolved.format(tb_suffix)
        self.experian_resolved = TableName.experian_resolved.format(tb_suffix)
        self.tivo_kantar_merged = TableName.tivo_kantar_merged.format(tb_suffix)
        
        tv_impressions = TableName.tv_impressions.format(self.duration, start_timestamp_str)

        ''' Internal variables for tivo in scripts to resolve '''
        raw_tivo = TableName.tivo_log
        start_date = self.start_date
        end_date = self.end_date
        start_date_int = date_utils.convert_date_fmt(start_date)
        end_date_int = date_utils.convert_date_fmt(end_date)
        kantar_tivo_dma_mapping = TableName.dma_mapping
        kantar_tivo_channel_mapping = TableName.channel_mapping
        # kantar_tivo_channel_mapping_mz = TableName.channel_mapping_mz
        # kantar_tivo_channel_mapping_basic = TableName.channel_mapping_basic
        

        ''' Internal variables for Kantar in scripts to resolve '''
        raw_kantar = TableName.kantar_log
        raw_creative = TableName.creative
        year = str(self.kantar_year)
        start_week = str(self.kantar_start_week)
        end_week = str(self.kantar_end_week)
        raw_market = TableName.market
        raw_oproduct = TableName.oproduct
        raw_product = TableName.product
        raw_product_name = TableName.product_name
        raw_property = TableName.property
        raw_media = TableName.media
        raw_advertiser = TableName.advertiser
        raw_oprog = TableName.oprog
        raw_prog = TableName.prog
        hosted_ultimateowner_id = TableName.hosted_ultimateowner_id


        ''' Internal variables for experian in scripts to resolve '''
        experian_mango = TableName.experian_mango
        experian_crosswalk = TableName.experian_crosswalk
        exp_start_date_int = str(self.exp_start_date_numeric)
        exp_end_date_int = str(self.exp_end_date_numeric)

        if ultimate_owner_id_list is None:
            if filter == 'ALL_ULTIMATEOWNER_ID':
                ultimateowner_id_join_filter=''
            elif filter == 'LIMITED_ULTIMATEOWNER_ID':
                ultimateowner_id_join_filter=Partial_HQL_Const.ultimateowner_id_filteration_join.format(
                    db=db,
                    hosted_ultimateowner_id=hosted_ultimateowner_id)
            else:
                ultimateowner_id_join_filter=Partial_HQL_Const.ultimateowner_id_filteration_join(
                    db=db,
                    hosted_ultimateowner_id=hosted_ultimateowner_id)
        else:
            ultmateowner_id_list_str=', '.join('\'{0}\''.format(w) for w in ultimate_owner_id_list)
            self.logger.info("ultmateowner_id_list_str: "+ultmateowner_id_list_str)
            ultimateowner_id_join_filter=Partial_HQL_Const.ultimateowner_id_list_filteration_join.format(ultmateowner_id_list_str=ultmateowner_id_list_str)

        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.hive_settings) \
                .append(HQL_Const.drop_view_if_exists, db=db, table=self.tivo_resolved) \
                .append(HQL_Const.drop_view_if_exists, db=db, table=self.kantar_resolved) \
                .append(HQL_Const.drop_view_if_exists, db=db, table=self.experian_resolved) \
                .append(HQL_Const.drop_view_if_exists, db=db, table=self.tivo_kantar_merged) \
                .append(HQL_Const.drop_view_if_exists, db=db, table=tv_impressions) \
                .append(Transform_HQL_Const.create_tivo_resolved_view, db=db, 
                        table=self.tivo_resolved,
                        raw_tivo = raw_tivo,
                        kantar_tivo_dma_mapping = kantar_tivo_dma_mapping,
                        kantar_tivo_channel_mapping = kantar_tivo_channel_mapping,
                        # kantar_tivo_channel_mapping_mz = kantar_tivo_channel_mapping_mz,
                        # kantar_tivo_channel_mapping_basic = kantar_tivo_channel_mapping_basic,
                        start_date_int = start_date_int,
                        end_date_int = end_date_int,
                        start_date = start_date,
                        end_date = end_date) \
                .append(Transform_HQL_Const.create_kantar_resolved_view, db=db, 
                        table=self.kantar_resolved,
                        raw_kantar = raw_kantar,
                        raw_creative = raw_creative,
                        raw_market = raw_market,
                        raw_oproduct = raw_oproduct,
                        raw_product = raw_product,
                        raw_product_name = raw_product_name,
                        raw_property = raw_property,
                        raw_media = raw_media,
                        raw_advertiser = raw_advertiser,
                        raw_oprog = raw_oprog,
                        raw_prog = raw_prog,
                        ultimateowner_id_join_filter=ultimateowner_id_join_filter,
                        #hosted_ultimateowner_id = hosted_ultimateowner_id,
                        year = year,
                        start_week = start_week,
                        end_week = end_week,
                        start_date = start_date,
                        end_date = end_date) \
                .append(Transform_HQL_Const.create_experian_resolved_view, db=db, 
                        table=self.experian_resolved,
                        experian_mango = experian_mango,
                        experian_crosswalk = experian_crosswalk,
                        start_date_int = exp_start_date_int,
                        end_date_int = exp_end_date_int) \
                .append(Transform_HQL_Const.create_tivo_kantar_merged_view, db=db, 
                        table=self.tivo_kantar_merged,
                        tivo_resolved=self.tivo_resolved,
                        kantar_resolved=self.kantar_resolved,
                        start_date = start_date,
                        end_date = end_date) \
                .append(Transform_HQL_Const.create_tv_impressions_view, db=db, 
                        table=tv_impressions,
                        tivo_kantar_merged=self.tivo_kantar_merged,
                        experian_resolved=self.experian_resolved) \

        transform_script = hql_builder.value()
        self.logger.debug("\n\n Transform Script: \n\n"+transform_script)
        return transform_script
        

    def cleanup_hive_intermediate_script(self, is_view=True):
        if is_view:
            return self.cleanup_intermediate_view_script()
        
        return self.cleanup_intermediate_tables_script()
    
    
    
    def cleanup_intermediate_view_script(self):

        db=System_Prop.staging_db
        start_timestamp_str = self.start_timestamp_str
        tv_impressions = TableName.tv_impressions.format(self.duration, start_timestamp_str)
        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.drop_view_if_exists, db=db, table=self.tivo_resolved) \
				.append(HQL_Const.drop_view_if_exists, db=db, table=self.kantar_resolved) \
				.append(HQL_Const.drop_view_if_exists, db=db, table=self.experian_resolved) \
				.append(HQL_Const.drop_view_if_exists, db=db, table=self.tivo_kantar_merged) \
				.append(HQL_Const.drop_view_if_exists, db=db, table=self.result_table) \

        return hql_builder.value()


    def cleanup_intermediate_tables_script(self):

        db=System_Prop.staging_db
        start_timestamp_str = self.start_timestamp_str
        tv_impressions = TableName.tv_impressions.format(self.duration, start_timestamp_str)
        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.drop_table_if_exists, db=db, table=self.tivo_resolved) \
				.append(HQL_Const.drop_table_if_exists, db=db, table=self.kantar_resolved) \
				.append(HQL_Const.drop_table_if_exists, db=db, table=self.experian_resolved) \
				.append(HQL_Const.drop_table_if_exists, db=db, table=self.tivo_kantar_merged) \
				.append(HQL_Const.drop_table_if_exists, db=db, table=self.result_table) \

        return hql_builder.value()

    
    def create_spark_transform_tables_script(self, ultimate_owner_id_list=None,
                               filter='LIMITED_ULTIMATEOWNER_ID'):

        db=System_Prop.staging_db
        #location=System_Prop.staging_s3_loc
        start_timestamp_str = self.start_timestamp_str
        tb_suffix = '{}_{}'.format(self.duration, self.start_timestamp_str)


        ''' Tables names for transform '''
        self.tivo_resolved = TableName.tivo_resolved.format(tb_suffix)
        self.kantar_resolved = TableName.kantar_resolved.format(tb_suffix)
        self.experian_resolved = TableName.experian_resolved.format(tb_suffix)
        self.tivo_kantar_merged = TableName.tivo_kantar_merged.format(tb_suffix)
        
        tv_impressions = TableName.tv_impressions.format(self.duration, start_timestamp_str)

        ''' Internal variables for tivo in scripts to resolve '''
        raw_tivo = TableName.tivo_log
        start_date = self.start_date
        end_date = self.end_date
        start_date_int = date_utils.convert_date_fmt(start_date)
        end_date_int = date_utils.convert_date_fmt(end_date)
        kantar_tivo_dma_mapping = TableName.dma_mapping
        kantar_tivo_channel_mapping = TableName.channel_mapping
        # kantar_tivo_channel_mapping_mz = TableName.channel_mapping_mz
        # kantar_tivo_channel_mapping_basic = TableName.channel_mapping_basic
        

        ''' Internal variables for Kantar in scripts to resolve '''
        raw_kantar = TableName.kantar_log
        raw_creative = TableName.creative
        year = str(self.kantar_year)
        start_week = str(self.kantar_start_week)
        end_week = str(self.kantar_end_week)
        raw_market = TableName.market
        raw_oproduct = TableName.oproduct
        raw_product = TableName.product
        raw_product_name = TableName.product_name
        raw_property = TableName.property
        raw_media = TableName.media
        raw_advertiser = TableName.advertiser
        raw_oprog = TableName.oprog
        raw_prog = TableName.prog

        if ultimate_owner_id_list is None:
            if filter == 'ALL_ULTIMATEOWNER_ID':
                ultimateowner_id_join_filter=''
            elif filter == 'LIMITED_ULTIMATEOWNER_ID':
                ultimateowner_id_join_filter=Partial_HQL_Const.ultimateowner_id_filteration_join.format(
                    db=db,
                    hosted_ultimateowner_id=TableName.hosted_ultimateowner_id)
            else:
                ultimateowner_id_join_filter=Partial_HQL_Const.ultimateowner_id_filteration_join(
                    db=db,
                    hosted_ultimateowner_id=TableName.hosted_ultimateowner_id)
        else:
            ultmateowner_id_list_str=', '.join('\'{0}\''.format(w) for w in ultimate_owner_id_list)
            self.logger.info("ultmateowner_id_list_str: "+ultmateowner_id_list_str)
            ultimateowner_id_join_filter=Partial_HQL_Const.ultimateowner_id_list_filteration_join.format(ultmateowner_id_list_str=ultmateowner_id_list_str)


        ''' Internal variables for experian in scripts to resolve '''
        experian_mango = TableName.experian_mango
        experian_crosswalk = TableName.experian_crosswalk
        exp_start_date_int = str(self.exp_start_date_numeric)
        exp_end_date_int = str(self.exp_end_date_numeric)

        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.hive_settings) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.tivo_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.kantar_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.experian_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.tivo_kantar_merged) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=tv_impressions) \
                .append(Transform_Spark_Const.create_tivo_resolved_table, db=db, 
                        table=self.tivo_resolved,
                        raw_tivo = raw_tivo,
                        kantar_tivo_dma_mapping = kantar_tivo_dma_mapping,
                        kantar_tivo_channel_mapping = kantar_tivo_channel_mapping,
                        # kantar_tivo_channel_mapping_mz = kantar_tivo_channel_mapping_mz,
                        # kantar_tivo_channel_mapping_basic = kantar_tivo_channel_mapping_basic,
                        start_date_int = start_date_int,
                        end_date_int = end_date_int,
                        start_date = start_date,
                        end_date = end_date) \
                .append(Transform_Spark_Const.create_kantar_resolved_table, db=db, 
                        table=self.kantar_resolved,
                        raw_kantar = raw_kantar,
                        raw_creative = raw_creative,
                        raw_market = raw_market,
                        raw_oproduct = raw_oproduct,
                        raw_product = raw_product,
                        raw_product_name = raw_product_name,
                        raw_property = raw_property,
                        raw_media = raw_media,
                        raw_advertiser = raw_advertiser,
                        raw_oprog = raw_oprog,
                        raw_prog = raw_prog,
                        ultimateowner_id_join_filter=ultimateowner_id_join_filter,
                        year = year,
                        start_week = start_week,
                        end_week = end_week,
                        start_date = start_date,
                        end_date = end_date) \
                .append(Transform_Spark_Const.create_experian_resolved_table, db=db, 
                        table=self.experian_resolved,
                        experian_mango = experian_mango,
                        experian_crosswalk = experian_crosswalk,
                        start_date_int = exp_start_date_int,
                        end_date_int = exp_end_date_int) \
                .append(Transform_Spark_Const.create_tivo_kantar_merged_table, db=db, 
                        table=self.tivo_kantar_merged,
                        tivo_resolved=self.tivo_resolved,
                        kantar_resolved=self.kantar_resolved) \
                .append(Transform_Spark_Const.create_tv_impressions_table, db=db, 
                        table=tv_impressions,
                        tivo_kantar_merged=self.tivo_kantar_merged,
                        experian_resolved=self.experian_resolved,
                        start_date = start_date,
                        end_date = end_date) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.tivo_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.kantar_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.experian_resolved) \
                .append(HQL_Const.drop_table_if_exists, db=db, table=self.tivo_kantar_merged) \

        transform_script = hql_builder.value()
        self.logger.debug("\n\n Transform Script: \n\n"+transform_script)
        return transform_script

    
    def __str__(self):
        return "Transformation from {} to {}".format(self.start_date, self.end_date)