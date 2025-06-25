import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import split, col, to_date, from_unixtime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Creating dynamic frame in order to handle nested schema
bronze_frames = glueContext.create_dynamic_frame.from_catalog(
    database="silver",
    table_name="silver-post",
    transformation_ctx="read_input"
    )

df = bronze_frames.toDF()
df = df.drop("subreddit.name")
df = df.withColumn("selftext_split", split(col("selftext"), " "))
df = df.withColumn("createddate", to_date(from_unixtime(col("createddate").cast("long"))))
dyf = DynamicFrame.fromDF(df, glueContext, "dyf_with_partitions")

#Writing to bronze zone with partition key = comment created day 
glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",
    connection_options={"path": "s3://luonghuy-datalakehouse/gold/post/", "partitionKeys": ["createddate"]},
    transformation_ctx="write_silver"
    )
job.commit()