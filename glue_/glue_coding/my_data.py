#import numpy as np 
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import when, col,lit
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField)

spark = SparkSession.builder.getOrCreate()

def get_data1():
    
    df = pd.read_csv("./datasets/mil-employees.csv")

    # df = df[:100000]

    return df

def get_data2():

    name_change_df = pd.read_csv("./datasets/name-change-final.csv")

    return name_change_df

def startpy():

    mySchema1 = StructType([
        StructField("", StringType(), False),
        StructField("id", StringType(), True),
        StructField("name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("country", StringType(), False),
        StructField("email", StringType(), False),
        StructField("unit", StringType(), False),
        StructField("experience", StringType(), False),
        StructField("domain", StringType(), False)
    ])

    mySchema = StructType([
        StructField("id", StringType(), True),
        StructField("names", StringType(), False)
    ])

    df_main = spark.createDataFrame(get_data1(),schema=mySchema1)

    #df_main.createOrReplaceTempView("data_main")

    df_name_change = spark.createDataFrame(get_data2(),schema=mySchema)

    #df_name_change.createOrReplaceTempView("data_name_change")

    df_final= df_main.join(df_name_change, on=['id'], how='outer')

    df_result= df_final.withColumn('name', when(df_final.names.isNull(),df_final.name).otherwise(df_final.names))

    df_result.drop("names","").orderBy("id").show()

    #df_result.toPandas().to_csv('./output/solution.csv')

if __name__ == '__main__':
    startpy()  