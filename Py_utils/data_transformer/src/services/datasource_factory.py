import logging
from models.ds_types import DSType
from models.datasource import Datasource

class DatasourceFactory:

    def __init__(self, ds_json_path: str):
        with open(ds_json_path, "r") as f:
            self.ds_dict = json.load(f)
        self.topological_sorted_list : list