import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit, current_date, when, col
from utils.constants import SILVER_POST

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Creating dynamic frame in order to handle nested schema
bronze_frames = glueContext.create_dynamic_frame.from_catalog(
    database="bronzedb",
    table_name="bronze-post",
    transformation_ctx="read_input"
    )

#ETL logic
df = bronze_frames.toDF()
df = df.withColumn("selftext",
    when((col("selftext") == "") | (col("selftext") == ""), lit(None)).otherwise(col("selftext"))
)
df = df.withColumnRenamed("created_utc", "createddate")
dyf = DynamicFrame.fromDF(df, glueContext, "dyf_with_partitions")

#Writing to bronze zone with partition key = comment created day 
glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",
    connection_options={"path": SILVER_POST, "partitionKeys": ["createddate"]},
    transformation_ctx="write_silver"
    )
job.commit()