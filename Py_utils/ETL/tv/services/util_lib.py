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

class Hive_Common_Util:

    logger = logging.getLogger(__name__)

    @classmethod
    def drop_table(cls, db=None, table = None,
                            dry_run=False, mock_run=False):
        if db is None or table is None:
            return
        hql_builder: HQLScript_Builder = HQLScript_Builder()
        hql_builder.append(HQL_Const.drop_table_if_exists, db=db, table=table) \

        hive_script = hql_builder.value()
        cls.logger.info("\n\n Drop table Script: \n\n"+hive_script)

        if not dry_run and not mock_run:
            cls.logger.info("\n\n Drop script is executing: \n\n")
            SQL.run_query(query=hive_script, raw=True, return_data=False)