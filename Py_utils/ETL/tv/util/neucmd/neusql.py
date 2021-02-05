from tv.util.neucmd.hive_query_neusql import HiveNeuSQL
from tv.util.neucmd.spark_query_neusql import SparkNeuSQL
from tv.util.qubole_hive_client import QuboleHiveClient
from tv.configs import Config



class SQL:

    @classmethod
    def run_query(cls,
                   query='show databases;',
                   config=None,
                   staging_dir=None,
                   raw=False,
                   nrows=1000,
                   return_data=False,
                   debug=False,
                   print_stderr=True,
                   query_conf_loc = None,
                   execution_engine = None,
                   **kwargs):

        
        '''
             Workaround as Result Headers is not working in Spark
        '''
        if return_data:
            execution_engine = 'HIVE'

        
        if execution_engine is None:
            execution_engine = Config.execution_engine
        if execution_engine is not None:
            execution_engine = execution_engine.upper()
        else:
            raise Exception("Please Provide execution_engine from (HIVE, SPARK, QUBOLE_HIVE )")

        if execution_engine =='SPARK' :
            return SparkNeuSQL.spark_query(query=query, config=config, staging_dir=staging_dir, raw=raw, 
                    nrows=nrows, return_data=return_data, debug=debug, print_stderr=print_stderr,
                    spark_config_loc = query_conf_loc, **kwargs)
        elif execution_engine =='HIVE' :
            return HiveNeuSQL.hive_query(query=query, config=config, staging_dir=staging_dir, raw=raw, 
                    nrows=nrows, return_data=return_data, debug=debug, print_stderr=print_stderr, **kwargs)
        elif execution_engine == 'QUBOLE_HIVE' :
            return QuboleHiveClient.hive_query(query=query, config=config, staging_dir=staging_dir, raw=raw, 
                    nrows=nrows, return_data=return_data, debug=debug, print_stderr=print_stderr, **kwargs)
        else:
            raise Exception("execution_engine: {}  is not supported yet \n Please Provide execution_engine from (HIVE, SPARK, QUBOLE_HIVE )".format(execution_engine))
    