import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import lit, current_date, col
from utils.constants import INPUT_POST, BRONZE_POST

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

#init
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Creating dynamic frame in order to handle nested schema
"""Using S3 source as a source file to avoid header problem"""
dynamic_frames = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [INPUT_POST],
        "recurse": True,
        "withHeader": True,
    },
    transformation_ctx="read_input"
)

#Converting to dataframe to build ETL pipeline
df = dynamic_frames.toDF()
df.printSchema()
df = df.withColumn("date", current_date())

#Converting back to dynamic frame
dyf = DynamicFrame.fromDF(df, glueContext, "dyf_with_partitions")

#Writing to bronze zone with partition key = today 
glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="csv",
    connection_options={"path": BRONZE_POST, "partitionKeys": ["date"]},
    transformation_ctx="write_bronze"
    )
job.commit()