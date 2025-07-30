# Crypto Timeseries
This repository contains a simple **ETL pipeline** that extracts cryptocurrency market data from the [CoinCap API 3.0](https://docs.coincap.io/) and loads it into a PostgreSQL database.

The *ETL workflow* has been implemented with different engines and workflow orchestrator:

- [main_etl_sqlalchemy.ipynb](link) -> database engine: [SQLAlchemy](https://www.sqlalchemy.org/)
- [main_etl_pyspark.ipynb](link) -> database engine: [Apache Spark](https://spark.apache.org/)
- [api_etl_database_dag.py](link) -> database engine: [Apache Spark](https://spark.apache.org/), workflow orchestrator: [Apache Airflow](https://airflow.apache.org/)

## Data
Data extracted and loaded into the database contain the following fields:

- timestamp:            timestamp
- id:                   string
- rank:                 int
- symbol:               string
- name:                 string
- supply:               double
- maxsupply:            double
- marketcapusd:         double
- volumeusd24hr:        double
- priceusd:             double
- changepercent24hr:    double
- vwap24hr:             double
- explorer:             string


## Data Pipeline
![](docs/etl_pipeline.png)

## Setup

### 0) Prerequisites
Make sure the following softwares have been installed on your local machine:
- [Python 3.9.6 ](https://www.python.org/downloads/release/python-396/)
- [Postgresql 16.3 ](https://www.postgresql.org/download/)
- [Apache Spark 3.5.4](https://spark.apache.org/releases/spark-release-3-5-4.html)

Make sure the setup is correct by executing their respective command line tools:
```shell
#Python
python --version

#Apache Spark
pyspark --version

#PostreSQL
psql --version
```

### 1) Virtual env
Prepare the Python virtual environment by:
```shell
pip install -r requirements.txt
```

## Execution
You can manually execute the notebooks under *notebooks* via Code Editors (such as VS Code) and selecting the virtual env created at 1).

To execute the Airlflow Dag:
- make sure the DAG is findable by your AIrflow installation:
```shell
export AIRFLOW__CORE__DAGS_FOLDER= <path to your repo instance>/dags
```

- Then, start the scheduler and webserver:
```shell
airflow scheduler
airflow webserver --port 8080
```

- Finally, trigger the *api_etl_pipeline_dag* manually from Airflow UI.
