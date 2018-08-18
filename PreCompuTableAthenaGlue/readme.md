We leverage Glue Python shell to convert low volume  Athena Query output   into data type enforced, Column compressed Parquet format. For bigger volumes Athena Query output , we leverage Glue Pyspark.

Usage
=====

import PreCompuTable

sql_query = """

SELECT a.listing_county,

         a.listing_state,

         a.listing_city,

         lower(trim(a.listing_status)) AS listing_status,

         count(distinct a.listing_id) AS lst_count,

         count( DISTINCT a.property_id) AS prop_count,

         min(a.listing_current_price) AS min_current_price,

         max(a.listing_current_price) AS max_current_price,

         count( DISTINCT a.listing_postal_code) AS postal_code_count

FROM  realestate.listings a  

GROUP BY  1, 2, 3, 4

""" 

env = 'dev'

target_table_name = 'ptas_test22'

s3_target_folder_parquet =  's3://bucketname/glue-ptas/pyspark-01/%s' %(target_table_name )

glue_crawler_name = 'ptas_test1' 

target_db_name = 'default'

glue_engine = 'pyspark'

#glue_engine = 'pyshell'

#---

ptas = PreCompuTable(sql_query = sql_query, 

                  target_db_name = target_db_name, 

                  target_table_name = target_table_name, 

                  s3_target_folder_parquet = s3_target_folder_parquet, 

                  env=env, 

                  glue_crawler_name = glue_crawler_name, 

                  glue_engine= glue_engine )


ptas.run_job() 


Setup
======
Create Python egg files for  dependencies:

- Trimmed down version of fastarquet (included only pure python features) package
	- Required for Parquet file preparation of  low volume datasets
- s3fs package
        - builds on boto3 to provide a convenient Python filesystem interface for S3 (https://github.com/dask/s3fs)


Copy Glue scripts and Python egg files to s3  
