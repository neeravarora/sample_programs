import re
import boto3
import logging
import sys
import os
import time
import threading
from boto3.s3.transfer import TransferConfig
from datetime import datetime

B = 1
KB = 1024
MB = KB * KB

class S3_OPs:
    S3_BUCKET_N_KEY_REGEX = 's3://([a-zA-Z0-9.\-_]{1,255}?)/(.*?)$'
    
    
    
    def __init__(self, access_key, secret_key, region_name='us-east-1'):
        self.log = logging.getLogger('S3 Ops')
        self.access_key=access_key
        self.secret_key=secret_key
        self.region_name=region_name
        self.s3_client = self.create_s3_client(self.access_key, self.secret_key, self.region_name)
    
    @classmethod
    def get_bucket_name(cls, s3_path):
        if re.match(cls.S3_BUCKET_N_KEY_REGEX, s3_path):
            matched = re.findall(cls.S3_BUCKET_N_KEY_REGEX, s3_path)
            return {'bucket' : matched[0][0],
                    'key' : matched[0][1],
                    'bucket_path' : 's3://{}/'.format(matched[0][0])}
        else:
            raise ValueError("Given S3 path : {} not a valid path".format(s3_path))
 

    @classmethod
    def get_full_s3_path(cls, bucket, key=''):
        if bucket is None or bucket == '':
            raise ValueError("Bucket for S3 path can't be None or Empty!")
        return 's3://{}/{}'.format(bucket, key)

    
    def list_subdirs_page(self, bucket, prefix='', delimiter='/', maxKeys=1000, continuationToken=None):
        if bucket is None or bucket == '':
            raise ValueError("Bucket for S3 path can't be None or Empty!")
        if continuationToken is None or continuationToken == '':
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter=delimiter, MaxKeys=maxKeys)
        else:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter=delimiter, 
                                                      MaxKeys=maxKeys, ContinuationToken=continuationToken)

        if response['KeyCount'] <=0: return {'result' : [],  'continuationToken' : None}

        if 'NextContinuationToken' in response:
            continuationToken = response['NextContinuationToken']
        else:
            continuationToken = None

        return {'result' : response['CommonPrefixes'],  'continuationToken' : continuationToken}
    

    def list_subdirs(self, bucket, prefix, delimiter='/', maxKeysPerReq=1000):
        continuationToken=''
        result=[]
        while(continuationToken != None):
            res = self.list_subdirs_page(bucket, prefix, delimiter=delimiter, 
                                                maxKeys=maxKeysPerReq, continuationToken=continuationToken)
            result.extend(res['result'])
            continuationToken = res['continuationToken']

        return result
    

    def list_page(self, bucket, prefix='', maxKeys=1000, continuationToken=None):
        if bucket is None or bucket == '':
            raise ValueError("Bucket for S3 path can't be None or Empty!")
        if continuationToken is None or continuationToken == '':
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=maxKeys)
        else:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, 
                                                      MaxKeys=maxKeys, ContinuationToken=continuationToken)

        if response['KeyCount'] <=0: return {'result' : [],  'continuationToken' : None}

        if 'NextContinuationToken' in response:
            continuationToken = response['NextContinuationToken']
        else:
            continuationToken = None

        return {'result' : response['Contents'],  'continuationToken' : continuationToken}


    def list_page_wrapper(self, bucket, prefix='', maxKeys=1000, continuationToken=None):
        if bucket is None or bucket == '':
            raise ValueError("Bucket for S3 path can't be None or Empty!")
        if continuationToken is None or continuationToken == '':
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=maxKeys)
        else:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, 
                                                      MaxKeys=maxKeys, ContinuationToken=continuationToken)
        if 'NextContinuationToken' in response:
            continuationToken = response['NextContinuationToken']
        else:
            continuationToken = None
                
        regex = '^{}/?(.*?)$'.format(prefix)
        res = [{'Key' : item['Key'],
                'RelativeFileName' : re.findall(regex, item['Key'])[0],
                'S3URL' : 's3://{}/{}'.format(bucket, item['Key']),
                'LastModifiedTimestamp':item['LastModified'].timestamp(),
                'LastModified':item['LastModified'],
                'LastModifiedDateTime':datetime.strftime(item['LastModified'], '%Y-%m-%d %H:%M:%S %s'),
                'Size' : item['Size'],
               }
               for item in response['Contents']]
        return {'result' : res,  'continuationToken' : continuationToken}


    def list_complete(self, bucket, prefix, maxKeysPerReq=1000):
        continuationToken=''
        result=[]
        while(continuationToken != None):
            res = self.list_page(bucket, prefix, maxKeys=maxKeysPerReq, continuationToken=continuationToken)
            result.extend(res['result'])
            continuationToken = res['continuationToken']

        return result


    def list_gen(self, bucket, prefix, maxKeysPerReq=1000):
        continuationToken=''
        while(continuationToken != None):
            res = self.list_page(bucket, prefix, maxKeys=maxKeysPerReq, continuationToken=continuationToken)
            continuationToken = res['continuationToken']
            yield res['result']

    
    def delete_file(self, bucket, key ):
        self.s3_client.delete_object(Bucket=bucket, Key=key)

    
    def copy(self, src, dest=None, src_size=None, src_s3_client = None):
        copy_src = self.get_bucket_name(src)
        copy_dest = self.get_bucket_name(dest)
        self.copy_by_bucketNkey(copy_src['bucket'], copy_src['key'],  
                                dest_bucket=copy_dest['bucket'], dest_key=copy_dest['key'], 
                                src_size=src_size, src_s3_client=src_s3_client)

    
    def copy_by_bucketNkey(self, src_bucket:str, src_key:str, 
                           dest_bucket:str=None, dest_key:str=None, 
                           src_size=None, src_s3_client = None):
        
        if src_bucket is None or dest_bucket is None or src_key is None or dest_key is None:
            raise ValueError("Invalid Args passed for this API.")
        
        config = TransferConfig(multipart_threshold= 5 * MB, max_concurrency=10, 
                                multipart_chunksize= 5 * MB, use_threads=True)
        copy_source = {
            'Bucket': src_bucket,
            'Key': src_key
        }
        
        progressMonitor = S3CopyProgress(self.get_full_s3_path(src_bucket, src_key), src_size)
        
        if src_s3_client is None:
            sourceClient=self.s3_client
        else:
            sourceClient = src_s3_client
        
        res = self.s3_client.copy(copy_source, dest_bucket, dest_key, 
    #                    ExtraArgs={'ContentType': 'text/html','ACL': 'public-read'},
                       SourceClient=sourceClient, 
                       Callback=progressMonitor.updateProgress,
                       Config=config )



    def file_obj(self, s3_path):
        s3_path_dict = self.get_bucket_name(s3_path)
        file_obj = self.file_obj_by_bucketNkey(s3_path_dict['bucket'], s3_path_dict['key'])
        return file_obj
    
    
    def file_obj_by_bucketNkey(self, bucket, key):
        file_obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        return file_obj
    
    
    def create_s3_client(self, access_key, secret_key, region_name='us-east-1'):
        s3_client = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
               # config=boto3.session.Config(signature_version='s3v4'),
                region_name=region_name
            )
        return s3_client
    
    
    def get_s3_client(self):
        return self.s3_client
    
    



class S3CopyProgress:
    def __init__(self, filename, src_size=None,):
        self.log = logging.getLogger('S3 Copy')
        self.filename = filename
        self.src_size = src_size
        self.bytesTransferredTotal = 0
 
    def updateProgress(self, bytesTransferred):
       
        self.bytesTransferredTotal += bytesTransferred
        
        if self.src_size is None:
            self.log.info("{} has copied {} bytes.".format(self.filename, str(self.bytesTransferredTotal)))
        else:
            percentComplete = round((self.bytesTransferredTotal/self.src_size)*100)
            self.log.info("{} has copied {}({}%) of {} bytes.".format(self.filename, 
                                                                        str(self.bytesTransferredTotal),
                                                                        str(percentComplete),
                                                                        str(self.src_size)))
            if self.bytesTransferredTotal == self.src_size:
                self.log.info("{} file has copied successfully.".format(self.filename))



# class S3CopyProgress:
#     def __init__(self, objectSummary):
#         self.log = logging.getLogger('S3 Copy')
#         self.objectSummary = objectSummary
#         self.bytesTransferredTotal = 0
 
#     def updateProgress(self, bytesTransferred):
       
#         self.bytesTransferredTotal += bytesTransferred
#         percentComplete = round((self.bytesTransferredTotal/self.objectSummary.size)*100)
#         sys.stdout.write('[%s] %s%s of %s\r' % (self.objectSummary.key, percentComplete, '%', self.objectSummary.size))
#         sys.stdout.flush()