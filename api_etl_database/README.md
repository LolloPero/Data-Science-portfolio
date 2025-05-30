# Crypto Timeseries
This repository contains a simple **ETL pipeline** that extracts cryptocurrency market data from the [CoinCap API](https://docs.coincap.io/) and loads it into a PostgreSQL database.

The *ETL workflow* has been written using 3 different engines:
- *SQLAlchemy* --> *main_etl_sqlalchemy.py*
- *PySpark* ------> *main_etl_pyspark.py*
- *AirFlow* -------> *dags/api_etl_database_DAG.py*

