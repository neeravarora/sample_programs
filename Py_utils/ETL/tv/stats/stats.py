import os, sys
import re
import time
import logging
import traceback
from datetime import datetime

from tv.ds_metadata import System_Prop, TableName, S3Location
from tv.hive_script import HQL_Const, HQLScript_Builder
from tv.util.neucmd.neusql import SQL

class StatUtil:

    find_creative_lmt_stat_query = "SELECT year, week FROM {db}.{table}"
    find_mta_impressions_query = "SELECT end_date FROM {db}.{table}"

    @classmethod
    def save_creative_lmt_stat(cls, year, week, timestamp, dry_run = True, mock_run=True):
        if dry_run or mock_run:
            return {}
        db = System_Prop.staging_db
        stat_table = TableName.stat_creative_lmt
        query = HQL_Const.insert_creative_stat.format(db=db, table = stat_table, \
                        year=year, week=week,timestamp=timestamp )
        SQL.run_query(query=query, raw=True, return_data=False)

    @classmethod
    def save_mta_impressions_stat(cls, start_date, end_date, timestamp, dry_run = True, mock_run=True):
        if dry_run or mock_run:
            return {}
        db = System_Prop.staging_db
        stat_table = TableName.stat_mta_tv_impressions
        query = HQL_Const.insert_impression_stat.format(db=db, table = stat_table, \
                        start_date=start_date, end_date=end_date,timestamp=timestamp )
        SQL.run_query(query=query, raw=True, return_data=False)

    @classmethod
    def get_creative_lmt_stat(cls, mock_run=True):
        if mock_run:
            return {}
        db = System_Prop.staging_db
        stat_table = TableName.stat_creative_lmt
        query = cls.find_creative_lmt_stat_query.format(db=db, table = stat_table )
        df = SQL.run_query(query=query, return_data=True)
        if df is not None and df['year'] is not None \
            and df['week'] is not None \
            and len(df['year']) >= 1 \
            and  len(df['week']) >= 1 :
            return {'year' : df['year'][0], 'week' : df['week'][0]}
        else : 
            return None

    @classmethod
    def get_mta_impressions_stat(cls, mock_run=True):
        if mock_run:
            return ''
        db = System_Prop.staging_db
        stat_table = TableName.stat_mta_tv_impressions
        query = cls.find_mta_impressions_query.format(db=db, table = stat_table )
        df = SQL.run_query(query=query, return_data=True)
        if df is not None and df['end_date'] is not None \
            and df['end_date'] is not None \
            and len(df['end_date']) >= 1  :
            return df['end_date'][0]
        else : 
            return None
    

    