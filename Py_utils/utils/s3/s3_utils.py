import subprocess
import os, sys
from threading import current_thread
from multiprocessing import current_process
import pandas as pd
import logging
from pathlib import Path
from utils.hive.hive_query import parse_config

def s3_put(
        df,
        config,
        s3_filename_prefix ="tmp_df",
        s3_filename_suffix="",
        s3_location=None,
        staging_location=None,
        debug=False,
        print_stderr=False,
        **kwargs):
    """Executes a s3 cmd using aws s3 CLI, captures stdout into a pd.DataFrame
    and optionally prints stderr into the screen

    Parameters
    ---------
    df : pandas.DataFrame
        DataFrame to be uploaded to Hive
    config : string
        Path to `platform_config.xml` file for the client for which the table will be created
    debug : boolean (Optional)
        If True, prints debug info to stdout, default: False
    print_stderr : boolean (Optional)
        If True, prints stderr into screen, default: False
    **kwargs will be directly passed to hive_query()
    """
    assert isinstance(df, pd.DataFrame), "df must be a pandas.DataFrame"
    assert isinstance(debug, bool), "debug must be boolean"
    assert isinstance(print_stderr, bool), "print_stderr must be boolean"

    # Parse config file
    platform, config_values = parse_config(config)
    logging.debug(config_values)
    assert platform.lower() in ('emr', 'qubole'), "Only EMR or Qubole platforms accepted"

    os.environ['AWS_ACCESS_KEY_ID'] = config_values['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config_values['secret_key']

    # Upload data to S3
    fname = '{}_{}_{}_{}.csv.gz'.format(s3_filename_prefix, current_process().ident, current_thread().ident, s3_filename_suffix)
    if staging_location is not None:
        fname = os.path.join(staging_location, fname)
    df.to_csv(fname, header=False, index=False, compression='gzip')
    
    if s3_location is None:
        s3_location = config_values['s3_location'] 
    #call_copy_data = f"aws s3 rm {s3_location} --recursive; aws s3 cp {fname} {s3_location}"
    call_copy_data = f"aws s3 cp {fname} {s3_location}"
    logging.info("S3 Command going to execute: \n{}".format(call_copy_data))
    p = subprocess.Popen(
        call_copy_data,
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        encoding='utf-8'
    )
    logging.info("stdout:\n {}".format(p.stdout.read()))
    if not p.stdout.read() == "":
        logging.error("stderr:\n {}".format(p.stderr.read()))

def s3_put_file(
        config,
        file_for_s3,
        s3_location=None,
        debug=False,
        print_stderr=False,
        **kwargs):
    """Executes a s3 cmd using aws s3 CLI, captures stdout into a pd.DataFrame
    and optionally prints stderr into the screen

    Parameters
    ---------
    df : pandas.DataFrame
        DataFrame to be uploaded to Hive
    config : string
        Path to `platform_config.xml` file for the client for which the table will be created
    debug : boolean (Optional)
        If True, prints debug info to stdout, default: False
    print_stderr : boolean (Optional)
        If True, prints stderr into screen, default: False
    **kwargs will be directly passed to hive_query()
    """
    #assert isinstance(df, pd.DataFrame), "df must be a pandas.DataFrame"
    assert isinstance(debug, bool), "debug must be boolean"
    assert isinstance(print_stderr, bool), "print_stderr must be boolean"

    # Parse config file
    platform, config_values = parse_config(config)
    logging.debug(config_values)
    assert platform.lower() in ('emr', 'qubole'), "Only EMR or Qubole platforms accepted"

    os.environ['AWS_ACCESS_KEY_ID'] = config_values['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config_values['secret_key']
    
    if s3_location is None:
        s3_location = config_values['s3_location']
    #call_copy_data = f"aws s3 rm {s3_location} --recursive; aws s3 cp {fname} {s3_location}"
    call_copy_data = f"aws s3 cp {file_for_s3} {s3_location}"
    p = subprocess.Popen(
        call_copy_data,
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        encoding='utf-8'
    )
    logging.info("stdout:\n {}".format(p.stdout.read()))
    if not p.stdout.read() == "":
        logging.error("stderr:\n {}".format(p.stderr.read()))
    
    
def s3_put_files_recursive(
        config,
        dir_for_s3,
        s3_location=None,
        debug=False,
        print_stderr=False,
        **kwargs):
    """Executes a s3 cmd using aws s3 CLI, captures stdout into a pd.DataFrame
    and optionally prints stderr into the screen

    Parameters
    ---------
    df : pandas.DataFrame
        DataFrame to be uploaded to Hive
    config : string
        Path to `platform_config.xml` file for the client for which the table will be created
    debug : boolean (Optional)
        If True, prints debug info to stdout, default: False
    print_stderr : boolean (Optional)
        If True, prints stderr into screen, default: False
    **kwargs will be directly passed to hive_query()
    """
    #assert isinstance(df, pd.DataFrame), "df must be a pandas.DataFrame"
    assert isinstance(debug, bool), "debug must be boolean"
    assert isinstance(print_stderr, bool), "print_stderr must be boolean"

    # Parse config file
    platform, config_values = parse_config(config)
    logging.debug(config_values)
    assert platform.lower() in ('emr', 'qubole'), "Only EMR or Qubole platforms accepted"

    os.environ['AWS_ACCESS_KEY_ID'] = config_values['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config_values['secret_key']
    
    if s3_location is None:
        s3_location = config_values['s3_location']
    #call_copy_data = f"aws s3 rm {s3_location} --recursive; aws s3 cp {fname} {s3_location}"
    call_copy_data = f"aws s3 cp {dir_for_s3} {s3_location} --recursive"
    logging.info("S3 Command going to execute: \n{}".format(call_copy_data))
    p = subprocess.Popen(
        call_copy_data,
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        encoding='utf-8'
    )
    logging.info("stdout:\n {}".format(p.stdout.read()))
    if not p.stdout.read() == "":
        logging.error("stderr:\n {}".format(p.stderr.read()))
    
def s3_remove_files_recursive(
        config,
        s3_location,
        debug=False,
        print_stderr=False):
    """Executes a s3 cmd using aws s3 CLI, captures stdout into a pd.DataFrame
    and optionally prints stderr into the screen

    Parameters
    ---------
    df : pandas.DataFrame
        DataFrame to be uploaded to Hive
    config : string
        Path to `platform_config.xml` file for the client for which the table will be created
    debug : boolean (Optional)
        If True, prints debug info to stdout, default: False
    print_stderr : boolean (Optional)
        If True, prints stderr into screen, default: False
    **kwargs will be directly passed to hive_query()
    """
    #assert isinstance(df, pd.DataFrame), "df must be a pandas.DataFrame"
    assert isinstance(debug, bool), "debug must be boolean"
    assert isinstance(print_stderr, bool), "print_stderr must be boolean"

    # Parse config file
    platform, config_values = parse_config(config)
    logging.debug(config_values)
    assert platform.lower() in ('emr', 'qubole'), "Only EMR or Qubole platforms accepted"

    os.environ['AWS_ACCESS_KEY_ID'] = config_values['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config_values['secret_key']
    
    if s3_location is None:
        return
    remove_dir_cmd = f"aws s3 rm {s3_location} --recursive"
    logging.info("S3 Command going to execute: \n{}".format(remove_dir_cmd))
    p = subprocess.Popen(
        remove_dir_cmd,
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        encoding='utf-8'
    )
    logging.info("stdout:\n {}".format(p.stdout.read()))
    if not p.stdout.read() == "":
        logging.error("stderr:\n {}".format(p.stderr.read()))
