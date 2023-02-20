
#import functions
import pytz
import sys
import boto3
import time
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.utils


args = getResolvedOptions(sys.argv, ['SUBJOB_ID','JOB_NAME','FILE_NAME'])

start=time.time()
dt_start=datetime.now().astimezone(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
print("job Start time",dt_start)


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['SUBJOB_ID'], args)

current_date=datetime.now().strftime("%Y-%m-%d")

client=boto3.client(service_name='glue',region_name='us-west-2',endpoint_url='https://glue.us-west-2.amazonaws.com')
s3_client=boto3.client('s3')


job_status_list=[("subjob_id","subjob_name","job_type","filename","batch_rundate","job_status","error_message","file_loc","dynamodb_update","job_start_time","job_end_time")]

validation_results_list=[("rule_id","subjob_id","batch_rundate","job_runid","validation_rule","validation_result","filename","source_cnt","target_cnt","comments")]



job_type='curation'
job_id=args['SUBJOB_ID']
file_loc="Src_Folder"
dynamodb_update= "Not Updated"



def sendNotification(sns_arn,sub,msg):
    sns_client=boto3.client('sns')
    sns_response=sns_client.publish(TopicArn=sns_arn,Message=msg,Subject=sub)
    
    
def writeJobStatus(job_status_bucket_path):
    print("Updating job status table")
    
    schema_struc=StructType([\
        StructField("subjob_id",IntegerType(),True),\
        StructField("subjob_name",StringType(),True),\
        StructField("job_type",StringType(),True),\
        StructField("filename",StringType(),True),\
        StructField("batch_rundate",StringType(),True),\
        StructField("job_status",StringType(),True),\
        StructField("error_message",StringType(),True),\
        StructField("file_loc",StringType(),True),\
        StructField("dynamodb_update",StringType(),True),\
        StructField("job_start_time",StringType(),True),\
        StructField("job_end_time",StringType(),True)])
        
    print(job_status_list)
    
    df_job_status=spark.createDataFrame(data=job_status_list[1:],schema=schema_struc)
    df_job_status=df_job_status.withColumn("batchdate",lit(current_date))
    df_job_status.show()
    job_status_frame=DynamicFrame.fromDF(df_job_status,glueContext,"job_status_frame")
    job_status_frame.show()
    print('===========p')
    
    sink=glueContext.getSink(connection_type="s3",path=job_status_bucket_path,enableUpdateCatalog=True,updateBehavior="UPDATE_IN_DATABASE",partitionKeys=["batchdate"])
    sink.setFormat("parquet",useGlueParquetWriter=True)
    sink.setCatalogInfo(catalogDatabase='audit_db',catalogTableName='job_status')
    sink.writeFrame(job_status_frame)
    print("job status updated successfully")
    
    
    

def writeValidationResults(validation_results_path):
    print("Updating validation result  table")
    print("job_status_list : ",validation_results_list)
    
    schema_struc=StructType([\
        StructField("rule_id",StringType(),True),\
        StructField("subjob_id",LongType(),True),\
        StructField("batch_rundate",StringType(),True),\
        StructField("job_runid",StringType(),True),\
        StructField("validation_rule",StringType(),True),\
        StructField("validation_result",StringType(),True),\
        StructField("filename",StringType(),True),\
        StructField("source_cnt",StringType(),True),\
        StructField("target_cnt",StringType(),True),\
        StructField("comments",StringType(),True)])
        
    print(validation_results_list)
    
    validation_results=spark.createDataFrame(data=validation_results_list[1:],schema=schema_struc)
    validation_results=validation_results.withColumn("batchdate",lit(current_date))
    validation_results.show()
    validation_results_frame=DynamicFrame.fromDF(validation_results,glueContext,"validation_results_frame")
    validation_results_frame.show()
    print('===========p')
    
    sink=glueContext.getSink(connection_type="s3",path=validation_results_path,enableUpdateCatalog=True,updateBehavior="UPDATE_IN_DATABASE",partitionKeys=["batchdate"])
    sink.setFormat("parquet",useGlueParquetWriter=True)
    sink.setCatalogInfo(catalogDatabase='audit_db',catalogTableName='job_validation')
    sink.writeFrame(validation_results_frame)
    print("validation result updated successfully")
    

    
    
    
print("job status")
job_status_bucket_path="s3://glue-lear/audit_folder/job_status/"
job_validation_path="s3://glue-lear/audit_folder/validation_results/"

job_status_list.extend([(1234, '', job_type, '', datetime.now(), 'new', 'error', file_loc, dynamodb_update, dt_start, datetime.now())])
validation_results_list.extend([('1234',1237,'ygt','shubham','chandani','vaishu','vanita','ashok','mama','mami')])

writeJobStatus(job_status_bucket_path)
print('job_status_done')

writeValidationResults(job_validation_path)
print('Validation table updated')

param_meta_data=glueContext.create_dynamic_frame.from_catalog(database='audit_db',table_name='param_details',transformation_ctx='job').toDF()
param_meta_data.createOrReplaceTempView("job_param_details")
df_paramDetail=spark.sql('select subjob_id, param_name,param_value from job_param_details' ).collect()
print(df_paramDetail)

param_dict={}
for param in df_paramDetail:
    param_key=param['param_name']
    param_value=param['param_value']
    print('key',param_key)
    print('value',param_value)
    param_dict[param_key]=param_value


arg_SNS_ARN=param_dict['SNS_ARN']
subject='my first sns mail'
msg='Hi shubham..if you are getting this mail then congrats'
sendNotification(arg_SNS_ARN,subject,msg)


print('hello world')
print('hello world2')