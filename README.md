# Sisu Airflow

Exports [Sisu API](https://docs.sisudata.com/docs/api/) results (currently, to Snowflake) via Airflow.

## Setup

Within your Python environment that you use to run Airflow please install [pysisu](https://github.com/sisudata/pysisu) via 
```
source <your airflow virtual env>
pip install pysisu
```

Note if you are using Snowflake you will need to install their connectors as well.
```
pip install snowflake-connector-python
pip install snowflake-sqlalchemy
pip install apache-airflow-providers-snowflake
```

## Importing into Airflow
Move the `sisu_snowflake.py` into whatever airflow instance you would like.
Simply copy the the file to your Airflow home directory to the folder `dags`.
