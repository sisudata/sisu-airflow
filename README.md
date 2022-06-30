# Sisu Airflow

This repo contains examples that bridge Sisu and Airflow together.

## Setup

Within your python environment that you use to run airflow please install [sisu-api](https://github.com/sisudata/pysisu) via 
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

## Importing Dags
You can move the `sisu_snowflake.py` into whatever airflow instance you would like.
Simply copy the the file over to your airflow home directory to the folder `dags`.