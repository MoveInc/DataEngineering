import boto3, sys, json, time, uuid, datetime, botocore, re
import threading
from multiprocessing.pool import ThreadPool
import traceback
import json
import sys



def my_print(msg):
    print("{} {}".format(datetime.datetime.utcnow(), msg))


# thread local data
thread_data = threading.local()

#------- 
import AthenaUtil

#----------
class preCompuTable(object):
    region_name = 'us-east-1'
    client = boto3.client(service_name='glue', region_name=region_name,
              endpoint_url='https://glue.us-east-1.amazonaws.com')

    glue_role = 'my_glue_role' 
    def __init__(self, sql_query=None ,target_db_name=None, target_table_name=None, env =None, s3_target_folder_parquet=None, 
                 glue_script_location=None, glue_dpu_instance_count=None, glue_custom_schema_location=None
                 , glue_crawler_name=None, glue_engine=None ):    
        """ Derive default  parameters based on inputs: target_table_name 
        """
                  
        self.env = env
        self.sql_query = sql_query
        self.target_db_name = target_db_name
        self.target_table_name = target_table_name 
        self.glue_crawler_name = glue_crawler_name
        assert self.sql_query, 'Required argument `sql_query` not found.'
        assert self.target_db_name, 'Required argument `target_db_name` not found.'
        assert self.target_table_name, 'Required argument `target_table_name` not found.'
        assert self.glue_crawler_name, 'Required argument `glue_crawler_name` not found.'



        #--- Stage Env
        s3_bucket_name = '<YourBucket/YourPrefix>' 
        etl_layer = 'glue-etl/athena_csv'
        run_instance = str( uuid.uuid1())
        self.s3_staging_folder_csv = "s3://%s-%s/%s/%s/%s" %(s3_bucket_name, self.env, etl_layer,  target_table_name,run_instance )
          
        etl_layer = 'glue-etl/parquet_data'
        #----
        if s3_target_folder_parquet is None:
            self.s3_target_folder_parquet = "s3://%s-%s/%s/%s" %(s3_bucket_name, self.env, etl_layer,  target_table_name)
        else: 
            self.s3_target_folder_parquet = s3_target_folder_parquet
 
        if glue_script_location is None:
            self.glue_script_location = 's3://<YourBucket/YourPrefix>/glue-etl/scripts/preCompuTable_csv2parquet.py' 
            self.glue_script_location_pyshell = 's3://<YourBucket/YourPrefix>/glue_python_shell/scripts/PyShellCSVToParquetFinal.py'
        else:
            self.glue_script_location = glue_script_location
            self.glue_script_location_pyshell = glue_script_location
            
        if glue_custom_schema_location is None:
            self.glue_custom_schema_location = 's3://<YourBucket/YourPrefix>/glue-etl/scripts/ctas_custom_schema.json'
        else:
            self.glue_custom_schema_location = glue_custom_schema_location            
            
        #glue_dpu_instance_count
        if glue_dpu_instance_count is None:
            self.glue_dpu_instance_count = 10 
        else:
            self.glue_dpu_instance_count = glue_dpu_instance_count
        if glue_engine is None:
            self.glue_engine = 'pyspark'
        else:
            self.glue_engine = glue_engine
            
        job_name = '.'.join([re.sub('[^0-9a-zA-Z]+', '',x).title()  for x in self.s3_target_folder_parquet.replace('s3://', '').split('/')[1:] ])
        self.glue_job_name = job_name  

    def athena_query_execute_save_s3(self )  :
        text = "AthenaUtil Initialization .."
        print text 
        util = AthenaUtil(s3_staging_folder = self.s3_staging_folder_csv)
        text = "Started athena_query_execute_save_s3 .."
        print text 
        print 'athena_query= ',  self.sql_query 
        if util.execute_save_s3(self.sql_query, self.s3_staging_folder_csv ):
            return True
        else:
            return False
    

    
    def  wait_for_job_to_complete(self, JobRunId):
        """ waits for Job to execute """
        text =  'Waiting for JobName = %s and \n JobId=%s  to Complete processing ...' %(self.glue_job_name, JobRunId)
        print text
#         self.banner(text)
        status = "STARTING"  # assumed
        error_count = 0
        response = None
        response =  self.client.get_job_run(JobName=self.glue_job_name, RunId=JobRunId)
        status = response["JobRun"]["JobRunState"] 
        while (status in ("QUEUED','RUNNING, STARTING")):  # 'JobRunState': 'STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'SUCCEEDED'|'FAILED',
            try:
                response =  self.client.get_job_run(JobName=self.glue_job_name, RunId=JobRunId)
                status = response["JobRun"]["JobRunState"] 
                # my_print(status)
                time.sleep(50)
            except botocore.exceptions.ClientError as ce:

                error_count = error_count + 1
                if (error_count > 3):
                    status = "FAILED"
                    print(str(ce))
                    break  # out of the loop
                if "ExpiredTokenException" in str(ce):
                    self.client = boto3.session.Session(region_name=self.region_name).client('glue')

        if (status == "FAILED" or status == "STOPPED"):
            # print(response)
            pass

        if response is None:
            return {"SUCCESS": False,
                    "STATUS": status }
        else:
            return  response
        
    def glue_etl_execute_csv2parquet(self):
        text =  'Starting glue_etl_execute_csv2parquet process  for %s ....' %(self.glue_job_name)
        print text
        print 's3_staging_folder_csv=', self.s3_staging_folder_csv
        print 's3_target_folder_parquet = ', self.s3_target_folder_parquet
        print ''
#       # Delete if job already exist
        self.client.delete_job(JobName = self.glue_job_name)
        #TODO Exception handling
        glue_job = self.client.create_job(Name = self.glue_job_name,  Role = self.glue_role, 
                                          AllocatedCapacity = self.glue_dpu_instance_count,
                                          ExecutionProperty={ 'MaxConcurrentRuns': 3 },
                                           Command={'Name': 'glueetl',
                                                    'ScriptLocation': self.glue_script_location}
                                         )
        #TODO Exception handling
        num_partitions = '10' 
        response = self.client.start_job_run(JobName = self.glue_job_name , Arguments = {
                 '--s3_location_csv_file':   self.s3_staging_folder_csv,
                 '--s3_location_parquet_file' : self.s3_target_folder_parquet, 
                 '--s3_location_custom_schema_json_file' : self.glue_custom_schema_location,
                 '--table_name' :  self.target_table_name,
                 '--num_partitions' : num_partitions })
        return response
    def glue_etl_execute_csv2parquet_pyshell(self):
        text =  'Starting glue_etl_execute_csv2parquet_pyshell process  for %s ....' %(self.glue_job_name)
        print text
        print 's3_staging_folder_csv=', self.s3_staging_folder_csv
        print 's3_target_folder_parquet = ', self.s3_target_folder_parquet
        print ''
#       # Delete if job already exist
        self.client.delete_job(JobName = self.glue_job_name)
        #TODO Exception handling
        glue_job = self.client.create_job(Name = self.glue_job_name,  Role = self.glue_role, 
                                          # AllocatedCapacity = self.glue_dpu_instance_count,
                                          ExecutionProperty={ 'MaxConcurrentRuns': 3 },
                                           Command={'Name': 'pythonshell',
                                             'ScriptLocation': self.glue_script_location_pyshell },
                                             DefaultArguments={'--extra-py-files': 's3://<YourBucket/YourPrefix>/glue_python_shell/eggs/fastparquet-0.1.5-py2.7_new.egg,s3://<YourBucket/YourPrefix>/glue_python_shell/eggs/s3fs-0.1.5-py2.7.egg'},
                                         )
        #TODO Exception handling
#         num_partitions = '10' 
        response = self.client.start_job_run(JobName = self.glue_job_name , Arguments = {
                 '--JOB_NAME': self.glue_job_name,
                 '--s3_location_csv_file':   self.s3_staging_folder_csv,
                 '--s3_location_parquet_file' : self.s3_target_folder_parquet, })
        return response
    
    def job_cleanup(self):
        print  'Job Cleanup step' 
        return self.client.delete_job(JobName = self.glue_job_name)
    
    def run_job(self):
        print 's3_staging_folder_csv=', self.s3_staging_folder_csv
        print 
        print 's3_target_folder_parquet = ', self.s3_target_folder_parquet
        print 
        print 'glue_script_location = ', self.glue_script_location
        print 
        print 'glue_custom_schema_location = ', self.glue_custom_schema_location
        print
        print 'Starting Athena Query Execution ...'
        print 
        if self.athena_query_execute_save_s3():
            ## Exit If Athena Query Fails 
            print 'Completed Athena Query Execution'
            print 
            print 'Starting Glue Job Execution ...'
            print 
            if self.glue_engine == 'pyspark':
                print ('Using %s' %self.glue_engine)
                response = self.glue_etl_execute_csv2parquet()
            else:    
                print ('Using %s' %self.glue_engine)
                response = self.glue_etl_execute_csv2parquet_pyshell()
            self.wait_for_job_to_complete(response['JobRunId'] )
            print 'Completed Glue Job Execution'
            print 

            self.update_start_and_wait_for_crawler_to_complete()
            self.job_cleanup()
            print 'Athena PTAS Done!'
            print 
