import boto3
import logging
import sys
import time
from libs.s3_ops import S3_OPs


class S3Stream:
    
    def __init__(self, access_key, secret_key):
        self.access_key=access_key
        self.secret_key=secret_key
        self.s3_ops_obj = S3_OPs(access_key, secret_key)
    
    def get_header(self, s3_path):
        return self.read_lines(s3_path, 1)
    
    def read_lines(self, s3_path, lines):
        stream = self.get_stream(s3_path)
        return self.read_lines_by_stream(stream, lines)
    
    def get_stream(self, s3_path):
        file_obj = self.s3_ops_obj.file_obj(s3_path)
        stream = file_obj['Body']
        return stream
    

    def read_lines_by_stream(self, stream, lines, chunk_size=16):
        chunk = stream.read(chunk_size)
        res = ''
        is_newline_rn = False
        while chunk != b'' and lines > 0:

            chunk_data = chunk.decode(encoding="utf-8",errors="ignore")
            #print(chunk_data)
            if "\\" in chunk_data or "\r" in chunk_data or "\r\\" in chunk_data:
                chunk = stream.read(chunk_size)
                chunk_data = chunk_data + chunk.decode(encoding="utf-8",errors="ignore")
                if "\r\n" in chunk_data:
                    lines = lines - 1
                    is_newline_rn = True
                elif "\n" in chunk_data:
                    lines = lines - 1
                    #print("==>" )
            elif "\r\n" in chunk_data:
                lines = lines - 1
                is_newline_rn = True
            elif "\n" in chunk_data:
                lines = lines - 1
                #print("=>" )


            res = res + chunk_data
            chunk = stream.read(chunk_size)
        #print("res=  " + res)
        if is_newline_rn: #res.rindex('\r\n') > -1:
            res = res[0: res.index('\r\n')]
        else:
            res = res[0: res.index('\n')]
        stream.close()
        return res