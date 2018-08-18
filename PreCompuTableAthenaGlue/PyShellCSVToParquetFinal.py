import json
import sys
import json
import sys

#----
import os
import s3fs
import pandas
import numpy as np
#-----        
import fastparquet
from fastparquet import ParquetFile
from fastparquet import write
import sys
reload(sys)
sys.setdefaultencoding('utf8')
#--------- Helper Classes ---
#--

import boto3
from boto3.s3.transfer import S3Transfer 
import os, uuid, time
from boto3.s3.transfer import TransferConfig
import threading, datetime, sys

class ProgressPercentage(object):
    def __init__(self, client, bucket, s3_key, local_file = None):
        # ... everything else the same
        
        if local_file is None:
            filename = s3_key
            # logger.info(filename)
            self._size = client.head_object(Bucket=bucket, Key=filename)["ContentLength"]
        else:
            filename = local_file
            # logger.info(filename)
            self._size = os.path.getsize(filename)
            
        self._filename = filename
        self._bucket = bucket
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._starttime = current_time1 = datetime.datetime.now()
        self._lasttime_secs = 0
    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            time_secs = (datetime.datetime.now() - self._starttime).total_seconds()
            self._seen_so_far += bytes_amount
            
            if (time_secs - self._lasttime_secs)<1:
                return
            
            speed_MBps = self._seen_so_far / (1024 * 1024 * time_secs)
            percentage = (self._seen_so_far / self._size) * 100
            
            
            sys.stdout.write(
                "\rCompleted %.2f MB/%.2f MB (%.2f MB/s)"
                % ( self._seen_so_far/(1024 * 1024.0), self._size/(1024 * 1024.0) , speed_MBps))
            sys.stdout.flush()
            self._lasttime_secs = time_secs  

class S3Helper(object):

    def get_file_size(self, s3_file_url):
        src_bucket, src_key_prefix = self.parse_s3_url(s3_file_url)
        s3 = boto3.client('s3')
        response = s3.head_object(Bucket=src_bucket, Key=src_key_prefix)
        return response['ContentLength']
    
    def upload_file(self, local_path, s3_file):
        
        bucket_name, s3_key = self.parse_s3_url(s3_file)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        KB = 1024
        MB = KB * KB

        transferConfig = TransferConfig(
                            max_concurrency=10,
                            num_download_attempts=5,
                            multipart_threshold=8 * MB,
                            multipart_chunksize=8 * MB,
                            max_io_queue=100,
                            io_chunksize=512 * KB,
                            use_threads=True)
        
        
        # logger.info("Uploading {} --> {}".format(local_path, s3_file)) 
        prog = ProgressPercentage( s3.meta.client , bucket_name, s3_key, local_path )
        bucket.upload_file(Filename = local_path, Key = s3_key,  Callback=prog, Config=transferConfig)
        
           
        
    def download_file(self, s3_file, local_path):
        
        bucket_name, s3_key = self.parse_s3_url(s3_file)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)

        KB = 1024
        MB = KB * KB
        
        transferConfig = TransferConfig(
                            max_concurrency=10,
                            num_download_attempts=5,
                            multipart_threshold=8 * MB,
                            multipart_chunksize=8 * MB,
                            max_io_queue=100,
                            io_chunksize=512 * KB,
                            use_threads=True)
        
        
        # logger.info("Downloading {} --> {}".format(s3_file, local_path))
        prog = ProgressPercentage( s3.meta.client , bucket_name, s3_key )
        bucket.download_file(Key=s3_key, Filename = local_path, Config=transferConfig, Callback=prog)


        
        
    def parse_s3_url(self, s3_url):
        s3_path_parts = s3_url.split('//')[1].split('/')
        s3_bucket_name = s3_path_parts[0]
        s3_key = "/".join(s3_path_parts[1:]) 
        return s3_bucket_name, s3_key

    def delete_files(self, s3_destn_prefix, simulate = False):
        if s3_destn_prefix.endswith('/'):
            s3_destn_prefix = s3_destn_prefix[:-1]
            
        client = boto3.client('s3', region_name='us-west-2')
        paginator = client.get_paginator('list_objects_v2')
        
        bucket, key_prefix = self.parse_s3_url(s3_destn_prefix)

        page_iterator = paginator.paginate(Bucket = bucket, Prefix = key_prefix)

        row_count = 0
        
        deleted_files = []
        for page in page_iterator:
            if "Contents" not in page:
                break
            for obj in page['Contents']:
                if obj['Size']>0:
                    # logger.info('deleting {}/{}'.format(bucket,obj['Key'] ))
                    if not simulate:
                        client.delete_object(Bucket=bucket,Key=obj['Key'])
                    deleted_files.append(obj['Key'])
                    row_count += 1
        return deleted_files

    def get_file_list(self, s3_src_prefix):
        '''
        returns list of s3 files under a s3 prefix
        @param s3_src_prefix:s3 prefix to get files from
        @return: list of files 
        '''
        
        if s3_src_prefix.endswith('/'):
            s3_src_prefix = s3_src_prefix[:-1]

        client = boto3.client('s3', region_name='us-west-2')
        paginator = client.get_paginator('list_objects_v2')
        
        src_bucket, src_key_prefix = self.parse_s3_url(s3_src_prefix)
                                               
        page_iterator = paginator.paginate(Bucket = src_bucket, Prefix = src_key_prefix)
        
        file_list = []
        for page in page_iterator:
            if "Contents" not in page:
                break
            for obj in page['Contents']:
                if obj['Size']>0:
                    file_list.append("s3://"+ src_bucket + "/" + obj['Key'])

        return file_list                        

    def copy_files(self, s3_src_prefix, s3_destn_prefix, overwrite=True, simulate = False, contains_partitioned_parquet_files = False):
        
        if s3_src_prefix.endswith('/'):
            s3_src_prefix = s3_src_prefix[:-1]
        if s3_destn_prefix.endswith('/'):
            s3_destn_prefix = s3_destn_prefix[:-1]
            
        client = boto3.client('s3', region_name='us-west-2')
        paginator = client.get_paginator('list_objects_v2')
        
        src_bucket, src_key_prefix = self.parse_s3_url(s3_src_prefix)
        destn_bucket, destn_key_prefix = self.parse_s3_url(s3_destn_prefix)
        
        
        if overwrite:
            self.delete_files(s3_destn_prefix, simulate)
                                               
        page_iterator = paginator.paginate(Bucket = src_bucket, Prefix = src_key_prefix)
        
        row_count = 0
        
        copied_files = []
        for page in page_iterator:
            if "Contents" not in page:
                break
            for obj in page['Contents']:
                if obj['Size']>0:
                    
                    target_key = destn_key_prefix + obj['Key'][len(src_key_prefix):]
                    
                    if contains_partitioned_parquet_files: #this is parquet files in hive format
                        if obj['Key'].endswith('_metadata'): #skip metadata files
                            continue

                        #get the suffix  after taking out the source prefix
                        key_suffix = obj['Key'][len(src_key_prefix):] 
                        if key_suffix.startswith('/'):
                            key_suffix = key_suffix[1:]
                            
                        #strip the indexes folder from key
                        key_suffix = '/' + '/'.join(key_suffix.split('/')[1:]) 
                        #add uuid if its a parquet file.
                        if key_suffix.endswith('parquet'):
                            key_suffix = key_suffix[:len(key_suffix)-len('parquet')] 
                            key_suffix = key_suffix + str(uuid.uuid1()) + '.parquet'
                        target_key = destn_key_prefix + key_suffix
                        
                    copySource = "{}/{}".format(src_bucket,obj['Key'])
                    # logger.info("copying {} --> {}/{}".format(copySource, destn_bucket, target_key))
                    copied_files.append(target_key)
                    if not simulate:
                        client.copy_object(Bucket=destn_bucket, CopySource= copySource, Key = target_key)
                    row_count = row_count + 1
        return copied_files 

#---- Function definitions --
def get_athena_query_output_s3_url( s3_folder):
    #download file from self.s3_staging_folder 
    s3_util = S3Helper()
    s3_file_list = s3_util.get_file_list( s3_folder )

    s3_csv_file = None
    csv_count = 0
    for s3_file in s3_file_list:
        if s3_file.endswith(".csv"):
            csv_count += 1
            if csv_count>1:
                raise Exception("Number of CSV files expected 1, found many {}".format(s3_file_list))
            s3_csv_file = s3_file

    assert s3_csv_file is not None, "No CSV file found here {}".format(s3_folder)
    return s3_csv_file
def get_header_type( header):  
    # Customize as needed
    return "str"

def get_type_dict_2(headers):
    type_dict = {} 

    for header in headers: 
            header_type =  get_header_type(header) 
            type_dict[header] = 'str' #header_type
    return type_dict 

def convert_csv_with_header_to_parquet( s3_url_for_csv_file, s3_output_folder):
        import os
        import s3fs
        import numpy as np
        import uuid
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        csv_file_to_parquet = s3_url_for_csv_file
        QueryExecutionId = str(uuid.uuid1())
        
        local_csv_file_name =  os.path.join('/tmp' , csv_file_to_parquet.split("/")[-1])
        local_parquet_file_name =  os.path.join('/tmp' , s3_output_folder.split("/")[-1]+ '.parquet' )
        s3_util = S3Helper()
        s3_util.download_file(csv_file_to_parquet, local_csv_file_name)
        csv_file_to_parquet = local_csv_file_name
                
        with open(csv_file_to_parquet) as f:
            first_line = f.readline()
            headers = first_line.split(",")
            if headers[0].startswith('"'):
                headers = [header.strip()[1 : -1] for header in headers]
            type_dict = get_type_dict_2(headers)
        df  = pandas.read_csv(csv_file_to_parquet, dtype=type_dict)  
        print df.head(20)
        df1 = df.head(300000).replace(np.nan, '', regex=True)
        print df1.head(20) 
        s3_key = s3_output_folder + ('/' if not s3_output_folder[-1] == '/' else '') + QueryExecutionId + '.parquet'
        try:
            write(local_parquet_file_name, df1, compression='GZIP')
            s3_util.upload_file( local_parquet_file_name, s3_key)
        finally:
            os.remove(csv_file_to_parquet)    
            os.remove(local_parquet_file_name) 

from awsglue.utils import getResolvedOptions 

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_location_csv_file', 
                           's3_location_parquet_file' ] )

s3_csv_folder = args['s3_location_csv_file']
s3_output_folder = args['s3_location_parquet_file']

print ('s3_csv_folder= ', s3_csv_folder)
print ('s3_output_folder= ',s3_output_folder)
s3_url_for_csv_file = get_athena_query_output_s3_url(s3_csv_folder)
print ('s3_url_for_csv_file= ',s3_url_for_csv_file)
convert_csv_with_header_to_parquet( s3_url_for_csv_file, s3_output_folder)  
