import findspark
import pandas as pd
import requests
import sys
import os
import pendulum
import time


from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
#from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
#from airflow.hooks.base_hook import BaseHook

from datetime import datetime
from pathlib import Path

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sqlalchemy import create_engine

#Include base directory
sys.path.append(Path(os.path.abspath(__file__)).parent.parent.as_posix())

from database_functions import (
    get_config,
    insert_values, 
    general_query)

#GLOBALS
POSTRESQL_JARFILE=r"/home/ubuntu/spark-3.5.1-bin-hadoop3/postgresql-42.7.3.jar"
URL_API = "http://api.coincap.io/v2/assets"
HEADER_API={
            "Content-Type":"application/json",
            "Accept-Encoding":"deflate" 
            }
CONFIG = get_config(filename="database.ini", section="crypto")
SPARK_SESSION_NAME="main_etl_pyspark"
TABLE_NAME_TARGET='crypto_timeseries'
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


#Define tasks
#extract
@task()
def extract():
    #api query
    api_response = requests.get(url=URL_API, headers=HEADER_API)
    api_response_data = api_response.json()

    return api_response_data

@task()
def transform(api_response_data: dict):

    #api json response to pandas Dataframe
    df = pd.json_normalize(data=api_response_data, record_path='data')

    #insert timestamp
    current_timestamp = datetime.now()
    current_timestamp.strftime('%d-%m-%Y %H:%M:%S')
    df['timestamp'] = [current_timestamp]*df.shape[0]
    df['timestamp'] = df['timestamp'].astype(str)

    #rename columns to lowercase
    rename_cols_dict={c:c.lower() for c in df.columns.tolist()}
    df.rename(columns=rename_cols_dict, inplace=True)

    df_dict = df.to_dict('dict')
    return df_dict    


@task()
def load(df_dict: dict):

        #initiate spark session
    print(f"\nInit Spark session..")
    spark = SparkSession \
        .builder \
        .appName(SPARK_SESSION_NAME) \
        .config("spark.jars", POSTRESQL_JARFILE) \
        .getOrCreate()

    print(f"\nSpark is running at: \n{spark._jsc.sc().uiWebUrl().get()}")


    #dict --> pandas Dataframe --> spark DataFrame
    pdf = pd.DataFrame(df_dict)
    dfs = spark.createDataFrame(pdf)

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

    m={"data loaded successfully"}

    print(f"""
                {m}

            """)    

    return m




with DAG(   dag_id="api_etl_database_dag",
            schedule="0 0 * * *", 
            start_date=pendulum.datetime(2024, 4, 25, tz="Europe/Copenhagen"),
            catchup=False,  
            tags=["api_etl_databse_TAG"]) as dag:

    api_response = extract() 

    api_response_transformed = transform(api_response)

    exit_status = load(api_response_transformed)

    api_response >> api_response_transformed >> exit_status
