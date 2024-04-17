import findspark
import os
import pandas as pd
import psycopg2
import requests


from database_functions import (
    get_config,
    insert_values, 
    general_query)
from datetime import datetime


#pyspark custom setup
os.environ['SPARK_HOME']=r"C:\Users\LPER0055\spark-3.5.1-bin-hadoop3"
os.environ['HADOOP_HOME']=rf"{os.path.join(os.environ['SPARK_HOME'],'hadoop')}"
os.environ["PYSPARK_PYTHON"]=r"C:\Users\LPER0055\Miniconda3\python.exe"
os.environ["PATH"]=";".join([   
                                fr"{os.environ['PATH']}",
                                fr"{os.path.join(os.environ['SPARK_HOME'],'bin')}",
                                fr"{os.path.join(os.environ['HADOOP_HOME'],'bin')}"
                            ])
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


#GLOBALS
TABLE_NAME_TARGET='crypto_timeseries'

CONFIG = get_config(filename="database.ini", section="crypto")

URL_API = "http://api.coincap.io/v2/assets"
HEADER_API={
            "Content-Type":"application/json",
            "Accept-Encoding":"deflate" 
            }

SPARK_SESSION_NAME="main_etl_pyspark"

COLUMN_DATATYPES = {
            "timestamp":            "timestamp",
            "id":                   "string",
            "rank":                 "int",
            "symbol":               "string",
            "name":                 "string",
            "supply":               "double",
            "maxsupply":            "double",
            "marketcapusd":         "double",
            "volumeusd24hr":        "double",
            "priceusd":             "double",
            "changepercent24hr":    "double",
            "vwap24hr":             "double",
            "explorer":             "string"
            }


def main() -> None:

    #initiate spark session
    print(f"\nInit Spark session..")
    spark = SparkSession \
        .builder \
        .appName(SPARK_SESSION_NAME) \
        .config("spark.jars", r"C:\Users\LPER0055\postgresql-42.7.3.jar") \
        .getOrCreate()

    print(f"\nSpark is running at: \n{spark._jsc.sc().uiWebUrl().get()}")


    ##----> ETRACT <----- ##
    print(f"\nEXTRACT from API query..")
    #api query
    response = requests.get(url=URL_API, headers=HEADER_API)


    ##----> TRANSFORM <----- ##
    print(f"\nTRANSFORM..")
    #api json response to pandas Dataframe
    responseData=response.json()
    df = pd.json_normalize(data=responseData, record_path='data')

    #insert timestamp
    current_timestamp = datetime.now()
    current_timestamp.strftime('%d-%m-%Y %H:%M:%S')
    df['timestamp'] = [current_timestamp]*df.shape[0]

    #rename columns to lowercase
    rename_cols_dict={c:c.lower() for c in df.columns.tolist()}
    df.rename(columns=rename_cols_dict, inplace=True)

    #pandas Dataframe --> spark DataFrame
    dfs = spark.createDataFrame(df)

    #assure expected columns data types
    for column_name, data_type in COLUMN_DATATYPES.items():
        dfs = dfs\
            .withColumn(column_name, col(column_name).cast(data_type))


    ##----> LOAD <----- ##
    print(f"\nLOAD..")
    #connect Spark to to postgreSQL database
    url_db = f"jdbc:postgresql://{CONFIG['host']}:{CONFIG['port']}/{CONFIG['database']}"
    properties_dbspark = {
            "user":     f"{CONFIG['user']}",
            "password": f"{CONFIG['password']}",
            "driver":   "org.postgresql.Driver"
        }

    #load PySpark DataFrame to postresql database
    dfs.write.jdbc(
                    url         =   url_db, 
                    table       =   TABLE_NAME_TARGET, 
                    mode        =   "append", 
                    properties  =   properties_dbspark
                )

if __name__ == "__main__":
    main()    