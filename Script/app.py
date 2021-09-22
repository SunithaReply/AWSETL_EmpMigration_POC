'''
Created on 
    
Course work: 
    
@author: 
Source:
    
Pip:
    pyspark
'''

# Import necessary modules
from pyspark.sql import SparkSession
import csv
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
import time
import requests
import json

spark = SparkSession.builder.getOrCreate()

URL = 'http://127.0.0.1:3009/get_data'

def get_data(file_name):
    
    df = pd.read_csv('static/' + file_name)

    return df

def store_data(df_result):

    df_result.toPandas().to_csv('static/results.csv')  

def create_spark_dataframe():

    vendor_1_df = spark.createDataFrame(get_data('sgs_vendor1.csv'))
    vendor_2_df = spark.createDataFrame(get_data('sgs_vendor2.csv'))

    vendor_1_df.createOrReplaceTempView("vendor_1_data")
    vendor_2_df.createOrReplaceTempView("vendor_2_data")

    result_df =  spark.sql("select product_id \
                            from vendor_1_data \
                            left join vendor_2_data \
                            ON vendor_1_data.product_id = vendor_2_data.KwikeeAssetId\
                            WHERE vendor_2_data.KwikeeAssetId is NULL")
    
    # store_data(result_df)
    result_df_pd = result_df.toPandas()

    add_data(result_df_pd)

def add_data(df):
    
    df_total = len(df)
  
    # df = pd.read_csv("static/results.csv")
    # df = df[0:5]

    #create DataFrame
    df1 = pd.DataFrame({
                'product_id': [],
                'gtin': [],
                'brand_name': [],
                'manufacturer_name' : [],
                'client_name' : []
            })

    print(df1)

    for index, product_id in enumerate(df["product_id"]):
        
        print("item no and index ", product_id, index)

        gtin, brand_name, manufacturer_name, client_name = get_product_detail(product_id)

        #define second DataFrame
        df2 = pd.DataFrame({
                        'product_id': [product_id],
                        'gtin': [gtin],
                        'brand_name': [brand_name],
                        'manufacturer_name' : [manufacturer_name],
                        'client_name' : [client_name]
                    })

        #add new row to end of DataFrame
        df1 = df1.append(df2, ignore_index = True)

    print(df1)

    df1.to_csv('static/results_with_producsts.csv')

def get_product_detail(productid):

    data = get_single_data()

    gtin                = data['gtin']
    brand_name          = data['brand_name']
    manufacturer_name   = data['manufacturer_name']
    client_name         = data['client_name']

    return gtin, brand_name, manufacturer_name, client_name

def get_single_data():

    new_data_dict = requests.get(URL)

    result = json.loads(new_data_dict.content)

    return result


if __name__ == '__main__':

    kai = time.perf_counter()

    # print(tic)

    create_spark_dataframe()

    pulla = time.perf_counter()

    # print(toc)

    # get_product_detail(123)

    print(f"time taken  using pyspark-sql in {pulla - kai:0.4f} seconds")

    print("check static folder for the results")

## Select <aushwal> from tabelleA A left join tabelleb B ON A.key=B.key where B.key is Null

## select product_id from vendor_1_data A left join vendor_2_data B ON A.product_id=B.product_id

## "SELECT * FROM vendor_1_data FULL OUTER JOIN vendor_2_data ON vendor_1_data.product_id = vendor_2_data.product_id"