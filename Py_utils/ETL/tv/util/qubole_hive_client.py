import os, sys
import time
import subprocess
import shlex
import json
import re
import gzip
import getpass
import threading
import logging
from queue import Queue
from threading import current_thread
from multiprocessing import current_process
import pandas as pd
import unittest
import traceback
import io
from lxml import etree
from tv import path_resolver
from tv.configs import Config

class QuboleHiveClient:

    logger = logging.getLogger(__name__)

    config = Config.get_json_configs()
    staging_dir = Config.staging_dir()
    
    @classmethod
    def hive_query(cls,
                query='show databases;',
                config : dict=None,
                cluster=None,
                raw=False,
                staging_dir=None,
                nrows=1000,
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
            cluster : string
                Cluster to be used (EMR and Qubole). Overrides cluster from platform config
            raw : boolean (Optional)
                If True, returns results as a string, default: False
            nrows : int (Optional)
                Number of rows to output into a DataFrame, default: 1000
                If non-positive or None, return all rows
                If raw==True, nrows is ignored and all data is returned
            debug : boolean (Optional)
                If True, prints debug info to stdout, default: False
            print_stderr : boolean (Optional)
                If True, prints stderr into screen, default: False
            **kwargs will be directly passed to pd.read_csv()
            """
            assert isinstance(query, str), "query must be a string"
            #assert os.path.isfile(config) or (config is None), "config must be a file or path or None"
            assert isinstance(config, dict) or (config is None), "config must be a dictionary or None"
            assert isinstance(cluster, str) or (cluster is None), "cluster must be a string or None"
            assert isinstance(nrows, int) or (nrows is None), "nrows must be int or None"
            assert isinstance(raw, bool), "raw must be boolean"
            assert isinstance(debug, bool), "debug must be boolean"
            assert isinstance(print_stderr, bool), "print_stderr must be boolean"
            
            if config is None:
                config = cls.config
            if not nrows:
                nrows = -1

            query = 'set hive.cli.print.header=true; set hive.resultset.use.unique.column.names=false; \n' + query

            if query[-1] != ';':
                query += ';'
            if debug:
                logging.info("** Query: \n{}".format(query))
            if staging_dir is None:
                staging_dir = Config.staging_dir()
            if staging_dir is not None:
                staging_dir = os.path.join(staging_dir, 'hive_query')
                os.makedirs(staging_dir, exist_ok=True)
                fname = '{}/tmp_hive_query_{}_{}_{}.hiveql'.format(staging_dir, int(time.time()), current_process().ident, current_thread().ident)
                
            else:
                fname = 'tmp_hive_query_{}_{}.hiveql'.format(current_process().ident, current_thread().ident)
            logging.debug("Hive query staging loc:"+fname)

            with io.open(fname, 'w', encoding='utf-8') as f:
                f.write(query)

            return cls.hive_query_qubole(config, fname, cluster, raw, nrows, debug, print_stderr, **kwargs)

    @classmethod
    def hive_query_qubole(cls,
                config,
                fname,
                cluster,
                raw,
                nrows,
                debug,
                print_stderr,
                **kwargs):
            cluster = cluster if cluster is not None else config['cluster']
            call_query = "qds.py --vv --url={url} --token '{token}' hivecmd submit --script_location {fname} --cluster={cluster}"
            call_query = call_query.format(url=config['url'], token=config['token'], fname=fname, cluster=cluster)

            logging.debug("** Call: {}".format(call_query))

            p = subprocess.Popen(
                shlex.split(call_query),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                encoding='utf-8'
            )
            job_id = cls.qubole_get_jobid(p.stdout)
            logging.info("Job ID: {}".format(job_id))

            # Using a new thread so printing stderr is non-blocking
            queue = Queue()
            if print_stderr:
                thr = threading.Thread(
                    target=cls.qubole_print_stderr,
                    args=(config, job_id, debug, queue)
                )
                thr.start()

            # Wait for the job to complete
            cls.qubole_wait_until_complete(config, job_id, debug, queue)

            # Get results
            results_handle = cls.qubole_get_results(config, job_id, debug)

            if raw:
                return results_handle.read()
            try:
                if nrows > 0:
                    data = pd.read_csv(results_handle, sep='\t', nrows=nrows, **kwargs)
                else:
                    data = pd.read_csv(results_handle, sep='\t', **kwargs)
                return data
            except Exception as e:
                # print_file(p.stderr)
                cls.logger.info("Failed creating a DataFrame with the following error: \n{}\n".format(e))
                if debug:
                    traceback.print_exc()
                return []

    @classmethod
    def qubole_get_jobid(cls, stdout):
        pattern = re.compile(r'Id:\s(\d{6,})')
        job_id = 0
        for line in stdout:
            match = pattern.search(line)
            if match:
                job_id = match.group(1)
                break
        return job_id

    @classmethod
    def qubole_wait_until_complete(cls, config, job_id, debug, queue):
        call = "qds.py --vv --url={url} --token '{token}' hivecmd check {id}"
        call = call.format(url=config['url'], token=config['token'], id=job_id)
        if debug:
            logging.info("** Call check status: {}".format(call))

        status = ''
        max_tries = 10000
        for _ in range(max_tries):
            time.sleep(15)
            queue.put("run")
            p = subprocess.Popen(
                shlex.split(call),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                encoding='utf-8'
            )
            raw_response = p.stdout.read()
            if debug:
                logging.debug("check query stderr:\n" + p.stderr.read())
                logging.debug("check query response: " + raw_response)
            
            try: 
                response = json.loads(raw_response)
                status = response['status']
            except Exception as e:
                if raw_response !='' or p.stderr.read() == '':
                    status = 'done'



            if status == 'done':
                queue.put("run")
                queue.put("exit")
                break
            elif status == 'error':
                queue.put("run")
                queue.put("exit")
                raise RuntimeError("Job {} failed with status {}".format(job_id, status))
        else:
            raise RuntimeError("Job {} did not complete in the maximum allowed time. Status {}".format(job_id, status))

    @classmethod
    def qubole_get_results(cls, config, job_id, debug):
        call = "qds.py --vv --url={url} --token '{token}' hivecmd getresult {id} true"
        call = call.format(url=config['url'], token=config['token'], id=job_id)
        if debug:
            logging.info("** Call get results: {}".format(call))

        p = subprocess.Popen(
            shlex.split(call),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        return p.stdout

    @classmethod
    def qubole_print_stderr(cls, config, job_id, debug, queue):
        call = "qds.py --vv --url={url} --token '{token}' hivecmd getlog {job_id}"
        call = call.format(url=config['url'], token=config['token'], job_id=job_id)

        if debug:
            logging.info("** Call: {}".format(call))

        max_lines = 0
        while True:
            command = queue.get()
            p = subprocess.Popen(
                shlex.split(call),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                encoding='utf-8'
            )
            for n, line in enumerate(p.stdout):
                if n >= max_lines:
                    logging.info(line, end='')
                    max_lines += 1
            if command == 'exit':
                break
