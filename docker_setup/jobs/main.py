from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import udf, regexp_replace
from config.config import configuration

from datetime import datetime
import re

def extract_p_id(file_content):
    file_content = file_content.strip()
    # First word of the file is the product id
    match = re.search(r'^(\w+)', file_content)
    p_id =  match.group(1) if match else None
    return p_id

def extract_p_name(file_content):
    # search for Product: <product_name>
    match = re.search(r'Product:\s*(.+)', file_content)
    p_name = match.group(1).strip() if match else None
    return p_name

def extract_p_category(file_content):
    # search for Category: <product_category>
    match = re.search(r'Category:\s*(.+)', file_content)
    p_category = match.group(1).strip() if match else None
    return p_category

def extract_p_price(file_content):
    # Search for Costs Rs.<price>
    match = re.search(r'Costs\s*Rs\.(\d+)', file_content)
    price = float(match.group(1)) if match else None
    return price

def extract_dates(file_content):
    match = re.search(r'Best From\s*(\d{4}-\d{2}-\d{2})\s*to\s*(\d{4}-\d{2}-\d{2})', file_content)
    if match:
        start_date = datetime.strptime(match.group(1), '%Y-%m-%d')
        end_date = datetime.strptime(match.group(2), '%Y-%m-%d')
        return start_date, end_date
    return None, None

def define_udfs():
    return {
        'extract_p_id_udf': udf(extract_p_id, StringType()),
        'extract_p_name_udf': udf(extract_p_name, StringType()),
        'extract_p_category_udf': udf(extract_p_category, StringType()),
        'extract_p_price_udf': udf(extract_p_price, DoubleType()),
        'extract_dates_udf': udf(extract_dates, StructType([
            StructField('p_created_date', DateType(), True),
            StructField('p_expiry_date', DateType(), True)
        ])),
    }

def streamWriter(input: DataFrame, checkpointFolder, output):
    return (
        input
        .writeStream
        .outputMode('append')
        .format('parquet')
        .option('path', output)
        .option('checkpointLocation', checkpointFolder)
        .trigger(processingTime='5 seconds')
        .start()
    )

if __name__ == '__main__':
    # Initialize the Spark session
    spark = (
        SparkSession.builder.appName('Unstructured_Data_Streaming_Spark_AWS')
        .config("spark.jars.packages", 
                'org.apache.hadoop:hadoop-aws:3.3.1,'
                'com.amazonaws:aws-java-sdk:1.11.469')
        .config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.access.key',configuration.get('AWS_ACCESS_KEY'))
        .config('spark.hadoop.fs.s3a.secret.key',configuration.get('AWS_SECRET_KEY'))
        .config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .getOrCreate()
    )

    # Variables for source files
    txt_src_dir = 'file:///opt/bitnami/spark/jobs/source_files/txt_source'
    csv_src_dir = 'file:///opt/bitnami/spark/jobs/source_files/csv_source'
    json_src_dir = 'file:///opt/bitnami/spark/jobs/source_files/json_source'

    # define data schema
    # p_id,p_name,p_category,p_price,p_create_date,p_expiry_date
    data_schema = StructType([
        StructField('p_id', StringType(), True),
        StructField('p_name', StringType(), True),
        StructField('p_category', StringType(), True),
        StructField('p_price', DoubleType(), True),
        StructField('p_created_date', DateType(), True),
        StructField('p_expiry_date', DateType(), True),
    ])

    # define udfs
    udfs = define_udfs()

    # reading from a file stream
    txt_file_df = (
        spark.readStream
        .format('text')
        .option('wholetext','true')
        .load(txt_src_dir)
    )

    txt_file_df = txt_file_df.withColumn('p_id', regexp_replace(udfs['extract_p_id_udf']('value'), r'\r',' '))
    txt_file_df = txt_file_df.withColumn('p_name', udfs['extract_p_name_udf']('value'))
    txt_file_df = txt_file_df.withColumn('p_category', udfs['extract_p_category_udf']('value'))
    txt_file_df = txt_file_df.withColumn('p_price', udfs['extract_p_price_udf']('value'))
    txt_file_df = txt_file_df.withColumn('p_create_date', udfs['extract_dates_udf']('value').getField('p_created_date'))
    txt_file_df = txt_file_df.withColumn('p_expiry_date', udfs['extract_dates_udf']('value').getField('p_expiry_date'))

    txt_parsed_df = txt_file_df.select('p_id', 'p_name', 'p_category', 'p_price', 'p_create_date', 'p_expiry_date')

    # reading from json source
    json_df = (
        spark.readStream
        .json(json_src_dir,schema=data_schema,multiLine=True)
    )

    # reading from csv source
    csv_df = (
        spark.readStream
        .format('csv')
        .option('header', 'true')
        .schema(data_schema)
        .load(csv_src_dir)
    )

    # union all the dataframes
    src_union_df = txt_parsed_df.union(json_df).union(csv_df)

    # to write the dataframe into s3 data lake in parquet format
    query = streamWriter(src_union_df, checkpointFolder='s3a://anish-shilpakar-bucket/checkpoints', output='s3a://anish-shilpakar-bucket/data/spark_unstructured/bronze')

    #to display the read stream in console as output
    # query = (
    #     src_union_df
    #     .writeStream
    #     .outputMode('append')
    #     .format('console')
    #     .option('truncate', False)
    #     .start()
    # ) 

    query.awaitTermination()

    spark.stop()