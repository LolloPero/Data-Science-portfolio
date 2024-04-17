import pandas as pd
import psycopg2
import requests


from database_functions import (
    get_config,
    insert_values, 
    general_query)
from datetime import datetime
from sqlalchemy import create_engine


#GLOBALS
config = get_config(filename="database.ini", section="crypto")
url_api = "http://api.coincap.io/v2/assets"
header_api={
            "Content-Type":"application/json",
            "Accept-Encoding":"deflate" 
        }



def main() -> None:

    ##----> ETRACT <----- ##
    #API query
    response = requests.get(url=url_api, headers=header_api)


    ##----> TRANSFORM <----- ##
    #API json response to pandas Dataframe
    responseData=response.json()
    df = pd.json_normalize(data=responseData, record_path='data')

    #insert timestamp
    current_timestamp = datetime.now()
    current_timestamp.strftime('%d-%m-%Y %H:%M:%S')
    df['timestamp'] = [current_timestamp]*df.shape[0]

    #rename columns to lowercase
    rename_cols_dict={c:c.lower() for c in df.columns.tolist()}
    df.rename(columns=rename_cols_dict, inplace=True)


    ##----> LOAD <----- ##
    #load data into postresql database
    conn_string = f"postgresql://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    
    db = create_engine(conn_string) 
    conn = db.connect() 

    #Insert Data
    df.to_sql(
                    name='crypto_timeseries',
                    con=conn,
                    if_exists='append',
                    index=True
                    )


if __name__ == "__main__":
    main()    