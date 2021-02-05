import os
from pathlib import Path

#PROJECT_ROOT_PATH = None
file_dir = Path(os.path.abspath( os.path.dirname(__file__)))
project_root_path = (file_dir / '..' ).resolve()
#os.environ['PROJECT_ROOT_PATH'] = str(project_root_path)

global PROJECT_ROOT_PATH
PROJECT_ROOT_PATH = str(project_root_path)


'''
path : relative path from project root dir
'''
def resolve(path: str):
    #Project root dir resolve
    
    
    if PROJECT_ROOT_PATH is None:
        # file_dir = Path(os.path.abspath( os.path.dirname(__file__)))
        # project_root_path = (file_dir / '..' ).resolve()
        # #os.environ['PROJECT_ROOT_PATH'] = str(project_root_path)
        # PROJECT_ROOT_PATH = str(project_root_path)
        # global PROJECT_ROOT_PATH
        print('Inside resolving project_root_path')
    
    #print('PROJECT_ROOT_PATH'+PROJECT_ROOT_PATH)
    abs_path = os.path.join(PROJECT_ROOT_PATH, path)
    #print('ABSOULUTE PATH'+abs_path)

    if os.path.exists(abs_path):
        return str(abs_path)

def get_project_root():
    return PROJECT_ROOT_PATH