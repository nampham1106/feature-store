import argparse
import os
import dotenv
import pandas as pd
from glob import glob
import pyspark
from pyspark.sql import Row, SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
import pyspark.sql
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType

dotenv.load_dotenv()
print("Starting PySpark job...")
minio_bucket = os.getenv("MINIO_BUCKET")

conf = pyspark.SparkConf().setMaster("local[*]").setAppName("PySparkApp")
conf.set("spark.jars.packages", 'org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-core_2.12:2.1.0')
conf.set('spark.hadoop.fs.s3a.endpoint', os.getenv('MINIO_ENDPOINT'))
conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('MINIO_ACCESS'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('MINIO_SECRET'))
conf.set('spark.hadoop.fs.s3a.path.style.access', "true")
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

sc = pyspark.SparkContext(conf=conf)
spark = pyspark.sql.SparkSession(sc)

df = spark.read.option("header", "true").csv("./batch_processing/test.csv")

df.show()


# df.write.csv(f"s3a://{minio_bucket}/test.csv", mode="overwrite")
