import os, sys
import time
import subprocess
import shlex
import json
import re
import gzip
import getpass
import threading
import logging
import glob
from queue import Queue
from threading import current_thread
from multiprocessing import current_process
import pandas as pd
import unittest
import traceback
import io
import configparser
from lxml import etree


class Config:

    staging_loc = None
    platform_config_loc = None

    logger = logging.getLogger(__name__)


    @classmethod
    def get_platform_config(cls, platform_config_loc=None):
        if platform_config_loc is not None:
            cls.platform_config_loc = platform_config_loc
            
        return cls.platform_config_loc


    @classmethod
    def staging_loc_path(cls, staging_loc= None):
        if staging_loc is not None:
            cls.staging_loc = str(staging_loc)
        return cls.staging_loc


