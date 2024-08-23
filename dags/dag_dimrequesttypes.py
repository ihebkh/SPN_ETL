import json
import psycopg2
from psycopg2 import OperationalError
from pymongo import MongoClient
from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta




def Dimrequesttypes(**context):
    requests_list_path = '/tmp/requests_list.json'
  

    file_paths = [requests_list_path]
    for path in file_paths:
        if not os.path.exists(path):
            raise ValueError(f"No data extracted or file does not exist at {path}")

    with open(requests_list_path, 'r') as f:
        requests_cleaned_data = json.load(f)

    postgres_conn_id = 'postgres'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_conn_str = f"dbname='{pg_conn.schema}' user='{pg_conn.login}' password='{pg_conn.password}' host='{pg_conn.host}' port='{pg_conn.port}'"
    postgres_conn = psycopg2.connect(pg_conn_str)
    

    insert_or_update_query = """
    INSERT INTO dimRequestTypes (reference, source, isb2b, req_type)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (reference) 
    DO UPDATE SET 
        source = EXCLUDED.source,
        isb2b = EXCLUDED.isb2b,
        req_type = EXCLUDED.req_type;
    """


    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for doc in requests_cleaned_data:
                reference = doc.get('reference')
                source = doc.get('source')
                isB2B = doc.get('isB2B')
                req_type = doc.get('services', [{}])[0].get('type') 
                
                if reference and source and req_type is not None: 
                    cursor.execute(insert_or_update_query, (reference, source, isB2B, req_type))
            


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 20, 0, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'load_dag_dimrequesttypes',
    default_args=default_args,
    description='Load data into PostgreSQL after extraction completes',
    catchup=False,
    schedule_interval='*/3 * * * *',
) as dag:
     wait_for_extraction = ExternalTaskSensor(
        task_id='wait_for_extraction',
        external_dag_id='extract_dag',
        external_task_id='finish_extraction',
        execution_delta=timedelta(minutes=0),
        timeout=600,
        poke_interval=10,
        mode='poke'
    )
     start_loading = DummyOperator(task_id='start_loading')
     finish_loading = DummyOperator(task_id='finish_loading')


     load_Dimrequesttypes = PythonOperator(
        task_id='Dimrequesttypes',
        python_callable=Dimrequesttypes,
       execution_timeout=timedelta(seconds=300), 
        do_xcom_push=True
    )

wait_for_extraction >> start_loading >> load_Dimrequesttypes >> finish_loading

