from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import json
import psycopg2
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta


def dim_region(**context):
   
    region_sa_path = '/tmp/region_SA.json'

    def load_json_file(file_path):
        if not file_path or not os.path.exists(file_path):
            raise ValueError(f"No data extracted or file does not exist at {file_path}")
        with open(file_path, 'r') as f:
            return json.load(f)


    region_sa_data = load_json_file(region_sa_path)

    postgres_conn_id = 'postgres'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_conn_str = (
        f"dbname='{pg_conn.schema}' user='{pg_conn.login}' "
        f"password='{pg_conn.password}' host='{pg_conn.host}' port='{pg_conn.port}'"
    )
    postgres_conn = psycopg2.connect(pg_conn_str)

    insert_update_region_query = """
    INSERT INTO dimregions (code_pays, pays)
    VALUES (%s, %s)
    ON CONFLICT (code_pays) 
    DO UPDATE SET pays = EXCLUDED.pays
    """

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for row in region_sa_data:
                code_pays = row['codepays']
                pays = row['pays']
                cursor.execute(insert_update_region_query, (code_pays, pays))

        postgres_conn.commit()

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
    'load_dag_dimRegion',
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

     dim_region_task = PythonOperator(
        task_id='DimRegion',
        python_callable=dim_region,
        execution_timeout=timedelta(seconds=300), 
        do_xcom_push=True
    )
     wait_for_extraction >> start_loading >> dim_region_task >> finish_loading

   
    
