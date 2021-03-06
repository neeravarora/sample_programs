#!~/anaconda/bin/python
"""
hive_query_neusql.py
Utility to run Hive queries and load results in a pandas DataFrame in multiple cluster infrastructures
Works in multiple platforms, including EMR, Qubole, and native Hadoop
"""

import subprocess, shlex
import sys
import os
import threading
import logging
from threading import current_thread
from multiprocessing import current_process
import pandas as pd
import unittest
import traceback
import io


def print_file(f):
    """Prints the content of a file or stream one line at a time"""
    for line in iter(f.readline, ''):
        logging.info(line, end='')
        sys.stdout.flush() # not being caught in notebook :(


def clean_query(query):
    """Cleans query and deletes comments"""
    clean = []
    for line in query.splitlines():
        if line.find('--') >= 0:
            clean.append(line[:line.find('--')].strip())
        else:
            clean.append(line.strip())
    return ' '.join(clean)


def hive_query(query='show databases;',
               config=None,
               staging_dir=None
               raw=False,
               nrows=1000,
               return_data=False,
               debug=False,
               print_stderr=False,
               **kwargs):
    """Executes a query using Hive CLI, captures stdout into a pd.DataFrame
    and optionally prints stderr into the screen

    Parameters
    ---------
    query : string
        Hive SQL query
    config : string
        Path to `platform_config.xml` file. If None, system will assume local Hive run
    raw : boolean (Optional)
        If True, returns results as a string, default: False
    nrows : int (Optional)
        Number of rows to output into a DataFrame, default: 1000
        If non-positive or None, return all rows
        If raw==True, nrows is ignored and all data is returned
    return_data : boolean (Optional)
        Set to True of the query is intended to return back data
    debug : boolean (Optional)
        If True, prints debug info to stdout, default: False
    print_stderr : boolean (Optional)
        If True, prints stderr into screen, default: False
    **kwargs will be directly passed to pd.read_csv()
    """
    assert isinstance(query, str), "query must be a string"
    assert os.path.isfile(config) or (config is None), "config must be provided"
    assert isinstance(raw, bool), "raw must be boolean"
    assert isinstance(return_data, bool), "return_data must be boolean"
    if not nrows: nrows = -1
    assert isinstance(nrows, int), "nrows must be int or None"
    assert isinstance(debug, bool), "debug must be boolean"
    assert isinstance(print_stderr, bool), "print_stderr must be boolean"
    assert 'NEUCMD_PATH' in os.environ, "path to NeuCMD must be in env variable 'NEUCMD_PATH'"

    if debug:
        print_stderr = True

    # Get NeuCMD location from the environment
    neucmd = os.environ['NEUCMD_PATH']
    # Add platform_config.xml location to the environment
    os.environ['PLATFORM_CONFIG_FILE'] = config

    # Convert query to single line if data must be returned
    if return_data:
        query = clean_query(query)

    # prepend Hive setting for returning headers
    query = 'set hive.cli.print.header=true; set hive.resultset.use.unique.column.names=false; \n' + query

    if query[-1] != ';':
        query += ';'

    if debug:
        logging.debug("** Query: \n{}".format(query))

    call = 'source {} && '.format(os.path.abspath(neucmd))
    if return_data:
        call = call + 'NeuSQL -S -e "{}"'.format(query)
    else:
        if staging_dir is not None:
            staging_dir = os.path.join(staging_dir, 'hive_query')
            os.makedirs(staging_dir, exist_ok=True)
            fname = '{}/tmp_hive_query_{}_{}.hiveql'.format(staging_dir, current_process().ident, current_thread().ident)

        else:
            fname = 'tmp_hive_query_{}_{}.hiveql'.format(current_process().ident, current_thread().ident)

        with io.open(fname, 'w', encoding='utf-8') as f:
            f.write(query)
        call = call + 'NeuSQL -f {}'.format(fname)

    if debug:
        logging.debug("** Call: {}".format(call))

    p = subprocess.Popen(
        call,
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        encoding='utf-8'
    )

    # Using a new thread so printing stderr is non-blocking
    if print_stderr:
        thr = threading.Thread(target=print_file, args=(p.stderr,))
        thr.start()

    if raw or not return_data:
        return p.stdout.read()
    try:
        if nrows > 0:
            data = pd.read_csv(p.stdout, sep='\t', nrows=nrows, **kwargs)
        else:
            data = pd.read_csv(p.stdout, sep='\t', **kwargs)
        return data
    except Exception as e:
        if not print_stderr:  # Force print stderr if an exception is found
            print_file(p.stderr)
        logging.error('Failed creating a DataFrame with the following error: \n{}\n'.format(e))
        if debug:
            traceback.print_exc()
        return []

# Unit tests:
class TestSuite(unittest.TestCase):

    def setUp(self):
        '''Sets up a test Hive DB and table'''
        self.table1 = pd.DataFrame([[10, 'Ada'], [100, 'Mia']], columns=['id', 'name'])
        self.table2 = pd.DataFrame([[10, 1], [100, 6]], columns=['id', 'age'])
        os.system("hive -e 'CREATE DATABASE hq_test;'")
        os.system("hive -e 'CREATE TABLE hq_test.table1 (id BIGINT, name STRING);'")
        os.system("hive -e 'CREATE TABLE hq_test.table2 (id BIGINT, age INT);'")
        os.system('''hive -e "INSERT INTO TABLE hq_test.table1 VALUES (10, 'Ada'), (100, 'Mia');"''')
        os.system("hive -e 'INSERT INTO TABLE hq_test.table2 VALUES (10, 1), (100, 6);'")

    def tearDown(self):
        '''Cleans Hive DB and table'''
        os.system("hive -e 'DROP TABLE IF EXISTS hq_test.table1 PURGE;'")
        os.system("hive -e 'DROP TABLE IF EXISTS hq_test.table2 PURGE;'")
        os.system("hive -e 'DROP DATABASE IF EXISTS hq_test;'")

    def test_get_dbs_df(self):
        data = hive_query(query='SHOW DATABASES;')
        self.assertIsInstance(data, pd.DataFrame, 'Result is not a pandas DataFrame')
        self.assertTrue(len(data)>0, 'No databases were found')

    def test_get_dbs_raw(self):
        raw = hive_query(query='SHOW DATABASES;', raw=True)
        self.assertIsInstance(raw, str, 'Result is not a string')
        self.assertTrue(len(raw.split('\n'))>1, 'No databases were found')

    def test_load_data(self):
        data = hive_query(query='SELECT * FROM hq_test.table1;')
        self.assertTrue(self.table1.equals(data), 'Loaded table1 incorrectly')
        data = hive_query(query='SELECT * FROM table1;', db='hq_test')
        self.assertTrue(self.table1.equals(data), 'Loaded table1 incorrectly (db)')

        data = hive_query(query='SELECT * FROM hq_test.table2;')
        self.assertTrue(self.table2.equals(data), 'Loaded table2 incorrectly')
        data = hive_query(query='SELECT * FROM table2;', db='hq_test')
        self.assertTrue(self.table2.equals(data), 'Loaded table2 incorrectly (db)')

    def test_load_data_raw(self):
        from io import BytesIO
        raw = hive_query(query='SELECT * FROM hq_test.table1;', raw=True)
        data = pd.read_csv(BytesIO(raw), sep='\t')
        self.assertTrue(self.table1.equals(data), 'Loaded raw table1 incorrectly')

    def test_merge(self):
        merged = self.table1.merge(self.table2, on = 'id')
        data = hive_query(query='SELECT t1.id, t1.name, t2.age FROM hq_test.table1 t1 JOIN hq_test.table2 t2 ON t1.id=t2.id;')
        self.assertTrue(merged.equals(data), 'Tables incorrectly merged')


if __name__ == '__main__':
    unittest.main()
