from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import udf, regexp_replace
from udf_utils import *
from config.config import configuration

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
    txt_src_dir = 'file:///Users/asus/Personal/Projects/de_unstructured_data_spark_streaming_aws/source_files/txt_source'
    csv_src_dir = 'file:///Users/asus/Personal/Projects/de_unstructured_data_spark_streaming_aws/source_files/csv_source'
    json_src_dir = 'file:///Users/asus/Personal/Projects/de_unstructured_data_spark_streaming_aws/source_files/json_source'

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

    #to display the read stream in console as output
    query = (
        src_union_df
        .writeStream
        .outputMode('append')
        .format('console')
        .option('truncate', False)
        .start()
    ) 

    query.awaitTermination()