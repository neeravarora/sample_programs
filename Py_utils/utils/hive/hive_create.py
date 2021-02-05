"""
hive_create.py
Utility to create tables in Hive from a pandas DataFrame in multiple cluster infrastructures
Works in multiple platforms, including EMR, Qubole, and native Hadoop
"""

import subprocess
import os
from threading import current_thread
from multiprocessing import current_process
import pandas as pd
import logging
from hive_query import hive_query, parse_config


def hive_create(
        df,
        table,
        db,
        config,
        store='PARQUET',
        queue=None,
        staging_dir=None,
        debug=False,
        print_stderr=False,
        **kwargs):
    """Executes a query using Hive CLI, captures stdout into a pd.DataFrame
    and optionally prints stderr into the screen

    Parameters
    ---------
    df : pandas.DataFrame
        DataFrame to be uploaded to Hive
    table : string
        Name of the table to be created
    db : string
        Name of the database in which the table will be created
    config : string
        Path to `platform_config.xml` file for the client for which the table will be created
    store : string
        Type of Hive storage to be used for created table
    queue : string or list of strings (Optional)
        Map-Reduce or TEZ queue to use. If list, the queue with most open capacity will be used
    debug : boolean (Optional)
        If True, prints debug info to stdout, default: False
    print_stderr : boolean (Optional)
        If True, prints stderr into screen, default: False
    **kwargs will be directly passed to hive_query()
    """
    assert isinstance(df, pd.DataFrame), "df must be a pandas.DataFrame"
    assert isinstance(table, str), "table must be a string"
    assert isinstance(db, str), "db must be a string"
    assert isinstance(store, str), "store must be a string"
    assert isinstance(debug, bool), "debug must be boolean"
    assert isinstance(print_stderr, bool), "print_stderr must be boolean"

    # Parse config file
    platform, config_values = parse_config(config)
    logging.info(config_values)
    assert platform.lower() in ('emr', 'qubole'), "Only EMR or Qubole platforms accepted"

    os.environ['AWS_ACCESS_KEY_ID'] = config_values['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config_values['secret_key']

    # Upload data to S3
    if staging_dir is not None:
        staging_data_dir = os.path.join(staging_dir, 'hive_data')
        os.makedirs(staging_data_dir, exist_ok=True)
        fname = '{}/tmp_df_{}_{}.csv.gz'.format(staging_data_dir, current_process().ident, current_thread().ident)
        
    else:
        fname = 'tmp_df_{}_{}.csv.gz'.format(current_process().ident, current_thread().ident)

    df.to_csv(fname, header=False, index=False, compression='gzip')
    s3_location = config_values['s3_location'] + db + ".db/" + table + "_ext/"
    call_copy_data = f"aws s3 rm {s3_location} --recursive; aws s3 cp {fname} {s3_location}"
    p = subprocess.Popen(
        call_copy_data,
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        encoding='utf-8'
    )
    logging.info("stdout:\n" + p.stdout.read())
    logging.info("stderr:\n" + p.stderr.read())

    # Create external hive table
    def type_mapping(x):
        if 'int' in str(x):
            return "BIGINT"
        elif 'float' in str(x):
            return "DOUBLE"
        else:
            return "STRING"

    columns = ',\n'.join(
        ['{} {}'.format(*c) for c in df.dtypes.map(type_mapping).items()]
    )

    query = """
    DROP TABLE IF EXISTS {db}.{table}_ext;
    DROP VIEW IF EXISTS {db}.{table}_ext;
    CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}_ext (
        {columns}
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{remote_path}'
    ;
    DROP TABLE IF EXISTS {db}.{table};
    DROP VIEW IF EXISTS {db}.{table};
    CREATE TABLE IF NOT EXISTS {db}.{table} STORED AS {store} AS
    SELECT * FROM {db}.{table}_ext
    ;""".format(
        db=db,
        table=table,
        store=store,
        columns=columns,
        remote_path=s3_location
    )

    if debug:
        logging.info("** Query: \n{}".format(query))

    hive_query(query, queue=queue, config=config, staging_dir= staging_dir, print_stderr=print_stderr, **kwargs)
