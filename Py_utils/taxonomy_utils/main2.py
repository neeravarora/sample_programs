import py_path
import sys, os
import argparse
import logging
import time
import json
from datetime import datetime
import traceback
from service import taxonomy_cs_api
from service.taxonomy_cs_api import Taxonomy_CS_API



if __name__ == '__main__':
    
    numeric_level = getattr(logging, 'DEBUG', None)
    stdout_handler = logging.StreamHandler(sys.stdout)
    logging.basicConfig(level=numeric_level,
                            format='%(asctime)s %(levelname)s %(name)s: %(message)s',
                            handlers=[stdout_handler])
    tcAPI = Taxonomy_CS_API()
    #a = taxonomy_cs_api.extract_src_detail(tcAPI, is_kernal_thread=True)
    a = tcAPI.extract_src_detail(is_kernal_thread=True)
    print("a" + str(a))
    
    
    
