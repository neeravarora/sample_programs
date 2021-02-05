from enum import Enum
import logging

class DSType(Enum):

    def __init__(self, **kwargs):
       self.param_dict = kwargs       
    
    """ 
        FIXED 
    """
    STATIC
    
    """ 
        NEWEST Latest file will be having all data No need to look at back 
    """
    LATEST
    
    """ MERGED Merge new and updated mapping with previous data """
    MERGED 
    
    """ 
        APPENDING new file as it is with previously processed. 
        An identifier will be added to determine appending file.
        For example arrival_date or some part from file name 
        which is unique among all the files already appended and will be appended in future.
    """
    HISTORICAL  

    """ 
        Source files will be selected based on parameters
    """
    SOURCE_SELECTED

    """ 
        Created using a handler top of one or more existing datasource 
        (currently handler will be implemented using hive query)
    """ 
    COMPOSITE  