import logging
from datetime import datetime
from typing import Type
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import sisu_api
from sisu_api.query_wrapper import Table
import os


API_KEY = os.environ.get('SISU_API_KEY')
TABLE_NAME = 'PUBLIC.SISU_ETL_EXAMPLE'
URL = 'https://dev.sisu.ai'
ANALYSIS_ID = 7340
SNOWFLAKE_CONNECTION_ID = 'snowflake_conn'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
args = {"owner": "Airflow", "start_date": datetime.today()}
dag = DAG(
    dag_id="sisu_snowflake_connector", default_args=args, schedule_interval=None
)


def upload_sisu_results(dwh_hook: SnowflakeHook, table: Table, table_name: str):
    insert_command = f'insert into {table_name} values '
    elements = [f'({str(row)})' for row in table.rows]
    insert_command += ','.join(elements) + ';'
    dwh_hook.run(insert_command)


def get_snowflake_type(python_type: Type) -> str:
    if python_type == str:
        return 'string'
    elif python_type == bool:
        return 'boolean'
    elif python_type == int:
        return 'integer'
    elif python_type == float:
        return 'double'
    elif python_type == datetime:
        return 'date'
    else:
        raise ValueError("unsupported type")


def create_sisu_results_table(dwh_hook: SnowflakeHook, table: Table, table_name: str):
    columns = [
        f'{column.column_name} {get_snowflake_type(column.column_type)}' for column in table.header]
    columns_rendered = ',\n'.join(columns)
    create_table = f'''
        CREATE OR REPLACE TABLE {table_name}(
        {columns_rendered}
        );
    '''
    dwh_hook.run(create_table)


def fetch_sisu_api(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION_ID)
    table = sisu_api.get_table(URL, ANALYSIS_ID, API_KEY)
    create_sisu_results_table(dwh_hook, table, TABLE_NAME)
    upload_sisu_results(dwh_hook, table, TABLE_NAME)


with dag:
    snowflake_test_connection = SnowflakeOperator(
        task_id="snowflake_test_connection",
        sql=['select 1;'],
        snowflake_conn_id=SNOWFLAKE_CONNECTION_ID,
    )

    count_query = PythonOperator(
        task_id="upload_data_from_sisu", python_callable=fetch_sisu_api)
snowflake_test_connection >> count_query
