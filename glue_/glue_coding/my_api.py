from flask import Flask, jsonify, request,render_template
import random
import csv
import numpy as np 
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import when, col,lit
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField)

app  = Flask(__name__)
PORT = 3009

spark = SparkSession.builder.getOrCreate()

'''
127.0.0.1/3009/get_data
'''

@app.route("/", methods = ["GET"])
def api_home():

    return render_template('index.html')

@app.route('/submit', methods = ["POST"])
def get_details():

    employee_id = request.values.get('id')

    unit = random.choice(['Engineering', 'Data Science', 'Predictive analysis frameworks','IT', 'Testing'])

    exp_factor = random.randint(1,3)
    
    experience = int(employee_id[-1])*exp_factor
    
    domain = random.choice(['ML', 'Software development', 'Data engineer', 'Frontend developer'])

    result = [{
        'id': "id_" + employee_id,
        'units'   : unit,
        'domains'   : domain,
        'experiences' : experience
    }]

    field_names = ['id', 'units', 'domains','experiences']

    with open('result.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = field_names)
        writer.writeheader()
        writer.writerows(result)

    df = pd.read_csv("./datasets/mil-employees.csv")

    name_change_df = pd.read_csv("result.csv")

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
        StructField("units", StringType(), False),
        StructField("experiences", StringType(), False),
        StructField("domains", StringType(), False)
    ])

    df_main = spark.createDataFrame(df,schema=mySchema1)

    df_main.createOrReplaceTempView("data_main")

    df_name_change = spark.createDataFrame(name_change_df,schema=mySchema)

    df_name_change.createOrReplaceTempView("data_name_change")

    df_final= df_main.join(df_name_change, on=['id'], how='outer')

    df_result= df_final.withColumn('unit', when(df_final.units.isNull(),df_final.unit).otherwise(df_final.units)) \
    .withColumn('domain', when(df_final.domains.isNull(),df_final.domain).otherwise(df_final.domains)) \
    .withColumn('experience', when(df_final.experiences.isNull(),df_final.experience).otherwise(df_final.experiences)) 

    df_result.drop("units","","domains","experiences").orderBy("id").toPandas().to_csv('./output/api_solution.csv')

    return render_template("index.html", results = result)


if __name__ == "__main__":
     app.run(debug = True, host = "0.0.0.0", port= 3009)