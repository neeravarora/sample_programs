import logging
from models.ds_types import DSType

class Datasource:
    
    def __init__(self, type: str):
        with open(ds_json_path, "r") as f:
            self.ds_dict = json.load(f)

    @property
    def type(self):
        return self.ds_type

class HiveDSOnHive(Datasource):
    
    def __init__(self, type: str):
        pass

class HiveDSOnS3(Datasource):

    def __init__(self, type: str):
        pass
