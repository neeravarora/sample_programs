import subprocess
import os, sys
import re
import logging
from tv.configs import Config


class S3:

    config = Config.get_json_configs()
    aws_access_key_id = config['access_key']
    aws_secret_access_key = config['secret_key']
    logger = logging.getLogger(__name__)

    '''
    Regex = '^.*1.gz$' end with ..1.gz
             '^part.*.gz$' with part..001.gz
    '''
    @classmethod
    def find_s3_files_by_regex(cls, 
            s3_location,
            regex,
            list_dir_cmd=None,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            debug=False,
            print_stderr=False):

        response = cls.list_s3_files(s3_location, aws_access_key_id, 
                aws_secret_access_key, debug, print_stderr)
        return list(filter(lambda x: re.match(regex, x), response))
    

    @classmethod
    def get_kantar_partition_metadata3(cls, res):
        regex = '^[0-9]{4}.*$'
        res_dict = {}
        def fun1(x: str, res_dict: dict = {}):
            s = re.match(regex, x)
            if s is not None:

                a = s.string.split('/tvweekly_')
                if a[0] not in res_dict:
                    res_dict[a[0]]=set()
                res_dict[a[0]].add(a[1])

        list(filter(lambda x: fun1(x, res_dict), res))
        cls.logger.debug("get_kantar_partition_metadata3()\n Res :\n"+str(res_dict))
        return res_dict


    @classmethod
    def get_kantar_partition_metadata(cls, res):
        regex = '^([0-9]{4}?)/tvweekly_0?([0-9]{1,2}?)$$'
        res_dict: dict = {}
        def fun2(x: str, res_dict: dict = {}):
            if re.match(regex, x):
                matched = re.findall(regex, x)
                year = matched[0][0]
                week = matched[0][1]
                if year not in res_dict:
                        res_dict[year]=set()
                res_dict[year].add(week)
        list(filter(lambda x: fun2(x, res_dict), res))
        cls.logger.debug("get_kantar_partition_metadata()\n Res :\n"+str(res_dict))
        return res_dict

    @classmethod
    def list_s3_files_kantar(cls,
            s3_location,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            debug=False,
            print_stderr=False):

        list_dir_cmd = f"aws s3 ls  {s3_location} --recursive"' | awk \'{print $NF}\'  | rev | cut -d \'/\' -f3- | rev | cut -d \'/\' -f3- | sort | uniq'
        return cls.list_s3_files(s3_location, list_dir_cmd, aws_access_key_id, aws_secret_access_key, debug, print_stderr)


    @classmethod
    def s3_ftp(cls,
            s3_location,
            local_dir,
            op_type = "DOWNLOAD",
            aws_access_key_id=None,
            aws_secret_access_key=None,
            debug=False,
            print_stderr=False):

        if op_type == 'DOWNLOAD':
            s3_cmd = f"aws s3 cp  {s3_location} {local_dir}"
        elif op_type =='UPLOAD':
            s3_cmd = f"aws s3 cp  {local_dir} {s3_location}"
        else:
            assert False, "Not Supported operation!"

        return cls.list_s3_files(s3_location, s3_cmd, aws_access_key_id, aws_secret_access_key, debug, print_stderr)


    @classmethod
    def list_s3_files(cls,
            s3_location,
            s3_cmd=None,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            debug=False,
            print_stderr=False):

        assert isinstance(debug, bool), "debug must be boolean"
        assert isinstance(print_stderr, bool), "print_stderr must be boolean"
        if aws_access_key_id is None or aws_secret_access_key is None:
            os.environ['AWS_ACCESS_KEY_ID'] = cls.aws_access_key_id
            os.environ['AWS_SECRET_ACCESS_KEY'] = cls.aws_secret_access_key
        else:
            os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
            os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
        
        if s3_location is None:
            return
        if s3_cmd is None:
            #list_dir_cmd = f"aws s3 ls {s3_location} --recursive| awk '"'{ if($3 >0) print $4}'"'"
            s3_cmd = f"aws s3 ls {s3_location}" ' | awk \'{ print $NF}\''
        
        cls.logger.info('s3 cmd: {}'.format(s3_cmd))
        p = subprocess.Popen(
            s3_cmd,
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        resStr = str(p.stdout.read())
        cls.logger.debug("stdout:\n" + resStr)
        cls.logger.debug("stderr:\n" + p.stderr.read())
        
        result = resStr.split('\n')
        return result