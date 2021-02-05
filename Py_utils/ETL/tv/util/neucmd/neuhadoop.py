import subprocess
import os, sys
import re
import logging
from pathlib import Path
from tv.configs import Config


class S3:

    logger = logging.getLogger(__name__)
    
    NEUCMD = "bash_scripts/Neu_cmd.sh"

    '''
    Regex = '^.*1.gz$' end with ..1.gz
             '^part.*.gz$' with part..001.gz
    '''
    @classmethod
    def find_files_by_regex(cls, 
            s3_location,
            regex,
            config=None,
            debug=False,
            print_stderr=False):

        response = cls.list_files(s3_location, config, debug, print_stderr)
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
    def list_kantar_files(cls,
            s3_location,
            config=None,
            debug=False,
            print_stderr=False):
        
        assert s3_location is not None and isinstance(s3_location, str), "s3_location must be string"
        neuhadoop_cmd = f"NeuHadoop -awss3cli -ls  {s3_location} --recursive"' | awk \'{print $NF}\'  | rev | cut -d \'/\' -f3- | rev | cut -d \'/\' -f3- | sort | uniq'
        
        return cls.__execute(neuhadoop_cmd, config, debug, print_stderr)

    
    @classmethod
    def copy(cls,
            from_location,
            to_location,
            op_type = "DOWNLOAD",
            config=None,
            recursive = False,
            debug=False,
            print_stderr=False):
        
        assert from_location is not None and isinstance(from_location, str), "from_location must be string"
        assert to_location is not None and isinstance(to_location, str), "to_location must be string"

        if op_type == 'DOWNLOAD':
            cls.__valid_local_path(to_location)
            cls.__valid_s3_path(from_location)
        elif op_type =='UPLOAD':
            cls.__valid_local_path(from_location)
            cls.__valid_s3_path(to_location)
        elif op_type =='TRANSFER':
            cls.__valid_s3_path(from_location)
            cls.__valid_s3_path(to_location)
        else:
            assert False, "Not Supported operation!"

        if recursive:
            neuhadoop_cmd =  f"NeuHadoop -awss3cli -cp  {from_location} {to_location} --recursive"
        else:
            neuhadoop_cmd =  f"NeuHadoop -awss3cli -cp  {from_location} {to_location}"

        return cls.__execute(neuhadoop_cmd, config, debug, print_stderr)


    @classmethod
    def list_files(cls,
            s3_location,
            config = None,
            debug=False,
            print_stderr=False):

        assert s3_location is not None and isinstance(s3_location, str), "s3 location must be string"
        neuhadoop_cmd = f"NeuHadoop -awss3cli -ls {s3_location}" ' | awk \'{ print $NF}\''
        return cls.__execute(neuhadoop_cmd, config, debug, print_stderr)

    @classmethod
    def execute(cls, 
            neuhadoop_cmd,
            config = None,
            debug=False,
            print_stderr=False):
        return cls.__execute(neuhadoop_cmd, config, debug, print_stderr)

        
    @classmethod
    def __execute(cls,
            neuhadoop_cmd,
            config = None,
            debug=False,
            print_stderr=False):

        assert isinstance(debug, bool), "debug must be boolean"
        assert isinstance(print_stderr, bool), "print_stderr must be boolean"
        assert neuhadoop_cmd is not None and isinstance(neuhadoop_cmd, str), "neuhadoop_cmd must be string"
        assert config is None or os.path.isfile(config), "config must be file"
        
        curr_dir = Path(os.path.abspath(os.path.dirname(__file__)))
        neucmd = os.path.join(curr_dir, cls.NEUCMD)
        # Add platform_config.xml location to the environment
        if config is None:
            config = Config.get_platform_config()
        os.environ['PLATFORM_CONFIG_FILE'] = config
        
        call = 'source {} && {}'.format(os.path.abspath(neucmd), neuhadoop_cmd)
        
        cls.logger.info('Neu hadoop cmd: {}'.format(neuhadoop_cmd))
        p = subprocess.Popen(
            call,
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

    @classmethod
    def __valid_s3_path(cls, s3_location=''):
        m = re.match("^s3://.*$","s3://tv-mta")
        assert m is not None, "{} should be valid s3 location format".format(s3_location)
        return True

    @classmethod
    def __valid_local_path(cls, path=''):
        p = os.path.exists(path)
        assert p, "{} should be valid local path location format".format(path)
        return True