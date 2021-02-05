import os, sys
import logging
import time
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

file_dir = Path(os.path.abspath( os.path.dirname(__file__)))
utils_path = (file_dir / '..' / '..' ).resolve()
if str(utils_path) not in sys.path:
    sys.path.append(str(utils_path))
#print('System path:'+ str(sys.path))