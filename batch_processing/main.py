from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType
import pandas as pd
from pyspark.ml import Pipeline
import psycopg2
from psycopg2_database_helper import upsert_spark_df_to_postgres

spark = (
        SparkSession.builder.appName("{database}_ddl")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )

dataframe = spark.read.option("header", "true").csv("s3a://diabetes/diabetes/diabetes.csv")
unlist = udf(lambda x: float(list(x)[0]), FloatType())
for column in dataframe.columns:
    dataframe = dataframe.withColumn(column, unlist(dataframe[column]))

normalized_features = [
    "Pregnancies",
    "BloodPressure",
    "SkinThickness",
    "Insulin",
    "Age",
]
dataframe.printSchema()
for feature in normalized_features:
    vector_assembler = VectorAssembler(
        inputCols=[feature],
        outputCol=f"{feature}_vect"
    )

    scaler = MinMaxScaler(
        inputCol=f"{feature}_vect", 
        outputCol=f"{feature}_normed"
    )
    pipeline = Pipeline(stages=[vector_assembler, scaler])
    dataframe = (
                pipeline.fit(dataframe)
                .transform(dataframe)
                .withColumn(f"{feature}_normed", unlist(f"{feature}_normed"))
                .drop(f"{feature}_vect", feature)
        )
    dataframe.show()

# write to postgreDB
conn_params = {
    'dbname': 'feature_store',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'localhost',
    'port': 5432
}

upsert_spark_df_to_postgres(dataframe, "features", None, conn_params)


dataframe.printSchema()
df_pandas = pd.DataFrame(dataframe.toPandas())
df_pandas.to_csv("test.csv")