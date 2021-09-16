import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField
)
from functools import reduce
from pyspark.sql.functions import col, udf, when
import random


spark = SparkSession.builder.getOrCreate()

S3_DATA_SOURCE_1_PATH = "s3://etlmigration-sun/data/200_employees.csv"
#S3_DATA_SOURCE_1_PATH = r'./200_employees.csv'
S3_DATA_SOURCE_2_PATH = "s3://etlmigration-sun/data/10-name-change .csv"
#S3_DATA_SOURCE_2_PATH = r'./10-name-change .csv'
S3_DATA_OUTPUT_PATH = "s3://etlmigration-sun/output_200emp/"
#S3_DATA_OUTPUT_PATH = r'./output_200emp.csv'


def get_unit():
    unit = 'Data Science'
    return unit


def get_experience():
    exp = random.randint(0, 20)
    return exp


def get_domain():
    domain = 'ML Consultant'
    return domain


def startpy():
    mySchema1 = StructType([

        StructField("id", StringType(), True),
        StructField("name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("country", StringType(), False),
        StructField("email", StringType(), False),
        StructField("unit", StringType(), False),
        StructField("experience", StringType(), False),
        StructField("domain", StringType(), False),
    ])

    mySchema = StructType([
        StructField("id", StringType(), True),
        StructField("names", StringType(), False)
    ])


    df_main = spark.read.schema(mySchema1).csv(S3_DATA_SOURCE_1_PATH)
    #df_main.show()
    df_new_employees = spark.read.schema(mySchema).csv(S3_DATA_SOURCE_2_PATH)
    df_new_employees = df_new_employees.where(df_new_employees.id != 'id')
    #df_new_employees.show()
    df_final = df_main.join(df_new_employees, on=['id'], how='left')
    #df_final.show()
    newcol = "Names"
    df_result = df_final.withColumn(newcol, when(df_final['names'].isNull(), df_final['name']).otherwise(df_final['names']))
    df_result = df_result.drop("name")
    df_result = df_result.where(df_result.id != 'id')
    df_order = df_result.select("id","Names","gender","address","country","email","unit","experience","domain")
    #df_order.show()
    df_order.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode('overwrite').save(
        f"{S3_DATA_OUTPUT_PATH}")
    return df_order


def check_time():
    # generate_fake_email()
    tic = time.perf_counter()
    startpy()
    toc = time.perf_counter()

    # print(f"Time: {toc - tic:0.4f} seconds")
    time_taken = toc - tic
    time_taken = str(f"{toc - tic}")
    print(f"time_taken is {time_taken}")
    return time_taken


if __name__ == '__main__':
    # startpy()
    check_time()