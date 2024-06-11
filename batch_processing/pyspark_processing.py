import os
from minio import Minio
from pyspark import SparkContext
from pyspark.sql import SparkSession

from minio import Minio

# spark = (
#         SparkSession.builder.appName("{database}_ddl")
#         .config("spark.executor.cores", "1")
#         .config("spark.executor.instances", "1")
#         .enableHiveSupport()
#         .getOrCreate()
#     )

client = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=True
)

minio_bucket = "mybucket"

found = client.bucket_exists(minio_bucket)
if not found:
    client.make_bucket(minio_bucket)

objects = client.list_objects('diabetes', prefix='diabetes/')
for obj in objects:
    print(obj.object_name)
    # dataframe = spark.read.option("header", "true").csv(f"s3a://diabetes/{obj.object_name}.csv")
    # dataframe.show()




