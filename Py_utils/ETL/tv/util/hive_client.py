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

class HiveClient:
    
    def hive_query(
            query='show databases;',
            config=None,
            cluster=None,
            queue=None,
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
        queue : string or list of strings (Optional)
            Map-Reduce or TEZ queue to use. If list, the queue with most open capacity will be used
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
        assert os.path.isfile(config) or (config is None), "config must be a file or path or None"
        assert isinstance(cluster, str) or (cluster is None), "cluster must be a string or None"
        assert isinstance(nrows, int) or (nrows is None), "nrows must be int or None"
        assert isinstance(raw, bool), "raw must be boolean"
        assert isinstance(debug, bool), "debug must be boolean"
        assert isinstance(print_stderr, bool), "print_stderr must be boolean"
        if not nrows:
            nrows = -1

        if config:
            platform, parameters = parse_config(config)
        else:
            platform = "local"
            parameters = None
        if debug:
            logging.info("Platform: {}".format(platform))
            logging.info("Parameters: {}".format(parameters))

        # prepend Hive setting for returning headers
        query = 'set hive.cli.print.header=true; set hive.resultset.use.unique.column.names=false; \n' + query

        # Defining a queue
        if platform in ("altiscale", "local") and queue is not None:
            query = select_queue(query, queue)

        if query[-1] != ';':
            query += ';'

        if debug:
            logging.info("** Query: \n{}".format(query))

        if staging_dir is not None:
            staging_dir = os.path.join(staging_dir, 'hive_query')
            os.makedirs(staging_dir, exist_ok=True)
            fname = '{}/tmp_hive_query_{}_{}.hiveql'.format(staging_dir, current_process().ident, current_thread().ident)
            
        else:
            fname = 'tmp_hive_query_{}_{}.hiveql'.format(current_process().ident, current_thread().ident)

        with io.open(fname, 'w', encoding='utf-8') as f:
            f.write(query)

        if platform in ("local", "altiscale"):
            return hive_query_local(fname, raw, nrows, debug, print_stderr, **kwargs)
        elif platform == "qubole":
            return hive_query_qubole(parameters, fname, cluster, raw, nrows, debug, print_stderr, **kwargs)
        elif platform == "emr":
            return hive_query_emr(parameters, fname, raw, nrows, debug, print_stderr, **kwargs)
        else:
            raise ValueError("platform {} unrecognized".format(platform))


    def hive_query_local(
            fname,
            raw,
            nrows,
            debug,
            print_stderr,
            **kwargs):
        call = 'hive -f {}'.format(fname)

        if debug:
            logging.info("** Call: {}".format(call))

        p = subprocess.Popen(
            shlex.split(call),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )

        # Using a new thread so printing stderr is non-blocking
        if print_stderr:
            thr = threading.Thread(target=print_file, args=(p.stderr,))
            thr.start()

        if raw:
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
            logging.info('Failed creating a DataFrame with the following error: \n{}\n'.format(e))
            if debug:
                traceback.print_exc()
            return []


    def hive_query_qubole(
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

        if debug:
            logging.info("** Call: {}".format(call_query))

        p = subprocess.Popen(
            shlex.split(call_query),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        job_id = qubole_get_jobid(p.stdout)
        logging.info("Job ID: {}".format(job_id))

        # Using a new thread so printing stderr is non-blocking
        queue = Queue()
        if print_stderr:
            thr = threading.Thread(
                target=qubole_print_stderr,
                args=(config, job_id, debug, queue)
            )
            thr.start()

        # Wait for the job to complete
        qubole_wait_until_complete(config, job_id, debug, queue)

        # Get results
        results_handle = qubole_get_results(config, job_id, debug)

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
            logging.info("Failed creating a DataFrame with the following error: \n{}\n".format(e))
            if debug:
                traceback.print_exc()
            return []


    def hive_query_emr(
            config,
            fname,
            raw,
            nrows,
            debug,
            print_stderr,
            **kwargs):

        # Add AWS keys to environment
        os.environ["AWS_ACCESS_KEY_ID"] = config['access_key']
        os.environ["AWS_SECRET_ACCESS_KEY"] = config['secret_key']

        # Update the cluster ID since it may have been restarted since config generation
        config['cluster'], master_node_addr = emr_get_cluster_id(config, debug)

        # Copy Hive query to S3
        s3_query_loc = "{}/scripts/{}/".format(
            config['s3_location'],
            getpass.getuser()
        )
        call_copy_query = f"aws s3 cp {fname} {s3_query_loc}"
        if debug:
            logging.info("call_copy_query: " + call_copy_query)
        p = subprocess.Popen(
            call_copy_query,
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        if debug:
            logging.info("copy query stdout:\n" + p.stdout.read())
            logging.info("copy query stderr:\n" + p.stderr.read())

        # Create step
        args = [
            "--cluster-id {}".format(config['cluster']),
            '--steps Type=Hive,Name="Hive_query_{2}",ActionOnFailure=CONTINUE,Args=[-f,{0},-d,INPUT={1},-d,OUTPUT={1}]'.format(
                s3_query_loc + fname,
                config['s3_location'],
                "{}_{}".format(current_process().ident, current_thread().ident)
            )
        ]
        call_run_query = 'aws emr add-steps {}'.format(
            " ".join(args)
        )
        if debug:
            logging.info("call_run_query: " + call_run_query)
        p = subprocess.Popen(
            call_run_query,
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        raw_response = p.stdout.read()
        if debug:
            logging.info("create step stderr:\n" + p.stderr.read())
            logging.info("create step response: " + raw_response)
        response = json.loads(raw_response)
        step_id = response['StepIds'][0]
        logging.info("step ID: " + step_id)

        # Using a new thread so printing stderr is non-blocking
        queue = Queue()
        if print_stderr:
            thr = threading.Thread(
                target=emr_print_stderr,
                args=(config, step_id, master_node_addr, debug, queue)
            )
            thr.start()

        # Wait until step completed
        job_status = emr_wait_until_complete(config, step_id, debug, queue)
        if job_status != "COMPLETED":
            if not print_stderr:
                stderr_handle = emr_get_results('stderr', config, step_id, master_node_addr, debug)
                logging.info("Job stderr:\n" + stderr_handle.read())
            raise RuntimeError("Job {} failed with status {}".format(step_id, job_status))

        # Get results
        results_handle = emr_get_results('stdout', config, step_id, master_node_addr, debug)

        if raw:
            return results_handle.read()

        try:
            if nrows > 0:
                data = pd.read_csv(results_handle, sep='\t', nrows=nrows, skiprows=1, **kwargs)
            else:
                data = pd.read_csv(results_handle, sep='\t', skiprows=1, **kwargs)
            return data
        except Exception as e:
            if not print_stderr:  # Force print stderr if an exception is found
                stderr_handle = emr_get_results('stderr', config, step_id, master_node_addr, debug)
                logging.info("Job stderr:\n" + stderr_handle.read())
            logging.info('Failed creating a DataFrame with the following error: \n{}\n'.format(e))
            if debug:
                traceback.print_exc()
            return []


    def parse_config(config):
        tree = etree.parse(config)
        platform = tree.xpath("/configroot/set[name='PLATFORM']/stringval")[0].text.lower()
        if platform == "qubole":
            fields = ['s3_location', 'token', 'url', 'cluster',
                    'access_key', 'secret_key']
        elif platform == "emr":
            fields = ['s3_location', 'access_key', 'secret_key', 'region',
                    'cluster_name', 'cluster', 'cluster_logs', 'JARS',
                    's3_aws_configuration_bucket', 'service_role',
                    'emr_release_label']
        else:
            fields = []
        result = {}
        for field in fields:
            result[field] = tree.xpath(
                "//{}_settings/set[name='{}']/stringval".format(platform, field)
            )[0].text.strip()
        return platform, result


    def emr_get_cluster_id(config, debug):
        name = config['cluster_name']
        region = config['region']
        call_get_clusters = f"aws emr list-clusters --active --region {region}"
        if debug:
            logging.info("call_get_clusters: " + call_get_clusters)
        p = subprocess.Popen(
            call_get_clusters,
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        p.wait()
        if p.returncode != 0:
            raise Exception("Failed to list clusters")
        raw_response = p.stdout.read()
        if debug:
            logging.info("list clusters stderr:\n" + p.stderr.read())
        clusters = json.loads(raw_response)
        active_clusters = [c for c in clusters['Clusters'] if
                        (c['Name'] == name) and (c['Status']['State'] in ("RUNNING", "WAITING"))]
        if debug:
            logging.info("active clusters:\n" + str(active_clusters))
        if len(active_clusters) > 1:
            raise Exception(f"More than one active cluster named {name}")
        if len(active_clusters) == 0:
            # Create a cluster
            logging.info("Active cluster {} not found. Creating a new one...".format(name))
            cluster_id = emr_create_cluster(config, debug)
            logging.info("Cluster {} created".format(cluster_id))
        else:
            cluster_id = active_clusters[0]['Id']
        master_node_addr = emr_get_master_node(cluster_id, debug)
        return cluster_id, master_node_addr


    def emr_create_cluster(config, debug):
        call_create_cluster_parts = [
            '''aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --tags Product="EMR"''',
            '''Environment="Prod" 'Account Name'={cluster_name} Project={cluster_name}''',
            '''--applications Name=Hadoop Name=Hive Name=Hue Name=Tez Name=Spark''',
            '''--configurations https://s3.amazonaws.com/{s3_aws_configuration_bucket}/Configuration.json''',
            '''--ec2-attributes https://s3.amazonaws.com/{s3_aws_configuration_bucket}/ec2_attributes.json''',
            '''--service-role {service_role} --enable-debugging --release-label {emr_release_label}''',
            '''--log-uri {cluster_logs} --name {cluster_name}''',
            '''--instance-group https://s3.amazonaws.com/{s3_aws_configuration_bucket}/InstanceGroupConfig.json''',
            '''--bootstrap-actions Path=s3://{s3_aws_configuration_bucket}/bootstrap_instances.sh''',
            '''--bootstrap-actions Path=s3://{s3_aws_configuration_bucket}/bootstrap_instances_master.sh''',
            '''--emrfs https://s3.amazonaws.com/{s3_aws_configuration_bucket}/emrfsconfig.json''']
        call_create_cluster = ' '.join(call_create_cluster_parts).format(**config)
        if debug:
            logging.info("call_create_cluster: " + call_create_cluster)
        p = subprocess.Popen(
            call_create_cluster,
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        p.wait()
        if p.returncode != 0:
            raise Exception("Failed to create cluster {}".format(config['cluster_name']))
        raw_response = p.stdout.read()
        if debug:
            logging.info("create cluster stderr:\n" + p.stderr.read())
        response = json.loads(raw_response)
        cluster_id = response['ClusterId']

        # Wait for cluster to be up and running
        status = emr_get_cluster_status(cluster_id, debug)
        while status not in ('WAITING', 'RUNNING'):
            logging.info("Cluster status: {}".format(status))
            time.sleep(20)
            status = emr_get_cluster_status(cluster_id, debug)
        return cluster_id


    def emr_get_cluster_status(cluster_id, debug):
        call_describe_cluster = f"aws emr describe-cluster --cluster-id {cluster_id}"
        if debug:
            logging.info("call_describe_cluster: " + call_describe_cluster)
        p = subprocess.Popen(
            call_describe_cluster,
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        p.wait()
        if p.returncode != 0:
            raise Exception("Failed to describe cluster {}".format(cluster_id))
        raw_response = p.stdout.read()
        if debug:
            logging.info("describe cluster stderr:\n" + p.stderr.read())
        response = json.loads(raw_response)
        return response['Cluster']['Status']['State']


    def emr_get_master_node(cluster_id, debug):
        call_describe_cluster = f"aws emr describe-cluster --cluster-id {cluster_id}"
        if debug:
            logging.info("call_describe_cluster: " + call_describe_cluster)
        p = subprocess.Popen(
            call_describe_cluster,
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        p.wait()
        if p.returncode != 0:
            raise Exception("Failed to describe cluster {}".format(cluster_id))
        raw_response = p.stdout.read()
        if debug:
            logging.info("describe cluster stderr:\n" + p.stderr.read())
        response = json.loads(raw_response)
        return response['Cluster']['MasterPublicDnsName']


    def emr_get_results(kind, config, step_id, master_node_addr, debug):

        # Get the file name with the query stdout
        file_search = f"/mnt/var/log/hadoop/steps/{step_id}/{kind}*".format(
            cluster_logs=config['cluster_logs'],
            cluster=config['cluster'],
            step_id=step_id
        )
        call_get_file_name = [
            'ssh -n -o "StrictHostKeyChecking no" -i {pem_key_path}',
            "hadoop@{master_node_addr} 'ls {file_search}'"
        ]
        call_get_file_name = " ".join(call_get_file_name).format(
            master_node_addr=master_node_addr,
            file_search=file_search,
            pem_key_path=os.path.expanduser('~/.ssh/emr-poc.pem')
        )
        if debug:
            logging.info(f"Get file name call: {call_get_file_name}")
        num_tries = 20
        for _ in range(num_tries):
            p = subprocess.Popen(
                call_get_file_name,
                shell=True,
                executable='/bin/bash',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                encoding='utf-8'
            )
            p.wait()
            ls_result = p.stdout.read()
            if debug:
                logging.info("stderr:\n" + p.stderr.read())
                logging.info("stdout:\n" + ls_result)
            split_ls_result = ls_result.strip().split('\n')
            if len(split_ls_result) > 1:
                raise ValueError(f"""More than one stdout file found in {master_node_addr}:{file_search}:
                {split_ls_result}""")
            if len(split_ls_result) == 0 and len(split_ls_result[0]) > 0:
                break
            else:
                time.sleep(5)
        file_path = split_ls_result[0]
        logging.info(f"Results path: {master_node_addr}:{file_path}")

        results_file = "{}_{}_{}".format(
            kind,
            current_process().ident,
            current_thread().ident
        )
        if file_path.endswith(".gz"):
            results_file += ".gz"
        call_get_results = [
            'scp -o "StrictHostKeyChecking no" -i {pem_key_path}',
            "hadoop@{master_node_addr}:{file_path} ./{results_file}"
        ]
        call_get_results = " ".join(call_get_results).format(
            master_node_addr=master_node_addr,
            file_path=file_path,
            results_file=results_file,
            pem_key_path=os.path.expanduser('~/.ssh/emr-poc.pem')
        )
        if debug:
            logging.info(f"Get results call: {call_get_results}")

        p = subprocess.Popen(
            call_get_results,
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        p.wait()
        if debug:
            logging.info("stderr:\n" + p.stderr.read())
            logging.info("stdout:\n" + p.stdout.read())

        if results_file.endswith(".gz"):
            results_handle = gzip.open(results_file, 'r')
        else:
            results_handle = open(results_file, 'r')
        return results_handle


    def emr_print_stderr(config, step_id, master_node_addr, debug, queue):
        max_lines = 0
        while True:
            command = queue.get()
            result_handle = emr_get_results('stderr', config, step_id, master_node_addr, debug)
            for n, line in enumerate(result_handle):
                if n >= max_lines:
                    logging.info(line, end='')
                    max_lines += 1
            if command == 'exit':
                break


    def emr_wait_until_complete(config, step_id, debug, queue):
        call_check_status = "aws emr describe-step --cluster-id {cluster} --step-id {step_id}".format(
            cluster=config['cluster'],
            step_id=step_id
        )
        if debug:
            logging.info("call_check_status: " + call_check_status)
        job_running = True
        job_status = None
        while job_running:
            p = subprocess.Popen(
                call_check_status,
                shell=True,
                executable='/bin/bash',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                encoding='utf-8'
            )
            p.wait()
            job_status = json.load(p.stdout)
            if debug:
                logging.info("job_status: \n" + json.dumps(job_status))
            if job_status['Step']['Status']['State'] in ("PENDING", "WAITING", "RUNNING"):
                time.sleep(10)
            else:
                job_running = False
            queue.put("run")
        queue.put("exit")
        if debug:
            logging.info(json.dumps(job_status))
        return job_status['Step']['Status']['State']


    def select_queue(query, queue):
        if isinstance(queue, str) and len(queue) > 0:
            query = ('set mapreduce.job.queuename={queue}; set tez.queue.name={queue};\n'.format(queue=queue)) + query
        elif isinstance(queue, list) and len(queue) > 0:
            queue_cap = {q: get_queue_capacity(q) for q in queue}
            logging.info('Queue capacities: {}'.format(queue_cap), file=sys.stderr)
            queue = max(queue_cap, key=queue_cap.get)
            logging.info('Using queue {}'.format(queue), file=sys.stderr)
            query = ('set mapreduce.job.queuename={queue}; set tez.queue.name={queue};\n'.format(queue=queue)) + query
        return query


    def get_queue_capacity(queue):
        """Runs a YARN command to get the available capacity for a queue
        Only works on local hadoop (Not on EMR or Qubole)"""
        call = 'yarn queue -status {}'.format(queue)
        p = subprocess.Popen(shlex.split(call),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=True)
        response = p.stdout.read()
        if 'Cannot get queue' in response:
            logging.info("Queue {} not found".format(queue), file=sys.stderr)
            return -1
        else:
            max_cap, curr_cap, cap = 0, 0, 0
            for line in response.splitlines():
                if 'Current Capacity' in line:
                    curr_cap = float(line.split(':')[1].strip().replace('%', '')) / 100
                elif 'Maximum Capacity' in line:
                    max_cap = float(line.split(':')[1].strip().replace('%', '')) / 100
                elif 'Capacity' in line:
                    cap = float(line.split(':')[1].strip().replace('%', '')) / 100
            return max_cap - (curr_cap * cap)


    def print_file(f):
        """Prints the content of a file or stream one line at a time"""
        for line in iter(f.readline, ''):
            logging.info(line, end='')
            sys.stdout.flush()  # not being caught in notebook :(


    def clean_query(query, remove_comments):
        """Cleans query and deletes comments"""
        clean = []
        for line in query.splitlines():
            if (line.find('--') >= 0) and remove_comments:
                clean.append(line[:line.find('--')].strip())
            else:
                clean.append(line.strip())
        return ' '.join(clean)


    def qubole_get_jobid(stdout):
        pattern = re.compile(r'Id:\s(\d{6,})')
        job_id = 0
        for line in stdout:
            match = pattern.search(line)
            if match:
                job_id = match.group(1)
                break
        return job_id


    def qubole_wait_until_complete(config, job_id, debug, queue):
        call = "qds.py --vv --url={url} --token '{token}' hivecmd check {id}"
        call = call.format(url=config['url'], token=config['token'], id=job_id)
        if debug:
            logging.info("** Call check status: {}".format(call))

        status = ''
        max_tries = 5000
        for _ in range(max_tries):
            time.sleep(5)
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
                logging.info("check query stderr:\n" + p.stderr.read())
                logging.info("check query response: " + raw_response)
            response = json.loads(raw_response)
            status = response['status']
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


    def qubole_get_results(config, job_id, debug):
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


    def qubole_print_stderr(config, job_id, debug, queue):
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




#==================================================
def create_hive_conf(config):
    tree = etree.parse(config)
    platform = tree.xpath("/configroot/set[name='PLATFORM']/stringval")[0].text.lower()
    
    if platform == "qubole":
        fields = ['s3_location', 'token', 'url', 'cluster',
                'access_key', 'secret_key']
        
    elif platform == "emr":
        fields = ['s3_location', 'access_key', 'secret_key', 'region',
                'cluster_name', 'cluster', 'cluster_logs', 'JARS',
                's3_aws_configuration_bucket', 'service_role',
                'emr_release_label']
    else:
        fields = []
    result = {}
    for field in fields:
        result[field] = tree.xpath(
            "//{}_settings/set[name='{}']/stringval".format(platform, field)
        )[0].text.strip()

    if platform == "qubole":
        return new QuboleHiveClusterConf(result['cluster'], result['s3_location'],
            result['access_key'], result['secret_key'], result['token'], result['url'])
    return None


class HiveClusterConf:

    def __init__(self, platform, cluster, s3_location, access_key, secret_key):
        self.platform = platform
        self.cluster = cluster
        self.s3_location = s3_location
        self.access_key = access_key
        self.secret_key = secret_key

    @property
    def platform(self):
        return self.platform

    @property
    def cluster(self):
        return self.cluster

    @property
    def s3_location(self):
        return self.s3_location

    @property
    def access_key(self):
        return self.access_key

    @property
    def secret_key(self):
        return self.secret_key

class QuboleHiveClusterConf(HiveClusterConf):

    def __init__(self, cluster, s3_location, access_key, secret_key, token, url):
        HiveClusterConf.__init__(self, "qubole", cluster, s3_location, access_key, secret_key)
        self.token = token
        self.url = url

    @property
    def token(self):
        return self.token

    @property
    def url(self):
        return self.url