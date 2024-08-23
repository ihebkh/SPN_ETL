from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor
import os
import json
import psycopg2
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

def Dimcurrency(**context):
    requests_list_path = '/tmp/requests_list.json'

    def load_json_file(file_path):
        if not file_path or not os.path.exists(file_path):
            raise ValueError(f"No data extracted or file does not exist at {file_path}")
        with open(file_path, 'r') as f:
            return json.load(f)

    requests_list_data = load_json_file(requests_list_path)

    postgres_conn_id = 'postgres'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_conn_str = f"dbname='{pg_conn.schema}' user='{pg_conn.login}' password='{pg_conn.password}' host='{pg_conn.host}' port='{pg_conn.port}'"
    postgres_conn = psycopg2.connect(pg_conn_str)

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for doc in requests_list_data:
                services = doc.get('services', [])
                if services:
                    benchmarks = services[0].get('benchmarks', [])
                    offers = services[0].get('offers', [])

                    for benchmark in benchmarks:
                        details = benchmark.get('details', [])
                        for detail in details:
                            currency = detail.get('currency', {})
                            code = currency.get('code')
                            label = currency.get('label')
                            if code and label:
                                cursor.execute(
                                    """
                                    INSERT INTO dimcurrency (code, label) 
                                    VALUES (%s, %s) 
                                    ON CONFLICT (code) 
                                    DO UPDATE SET label = EXCLUDED.label
                                    """,
                                    (code, label)
                                )

                    for offer in offers:
                        details = offer.get('details', [])
                        for detail in details:
                            currency = detail.get('currency', {})
                            code = currency.get('code')
                            label = currency.get('label')
                            if code and label:
                                cursor.execute(
                                    """
                                    INSERT INTO dimcurrency (code, label) 
                                    VALUES (%s, %s) 
                                    ON CONFLICT (code) 
                                    DO UPDATE SET label = EXCLUDED.label
                                    """,
                                    (code, label)
                                )

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
    'load_dag_dimcurrency',
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

    load_Dimcurrency = PythonOperator(
        task_id='Dimcurrency',
        python_callable=Dimcurrency,
        provide_context=True,
        execution_timeout=timedelta(seconds=300), 
        do_xcom_push=True
    )

    wait_for_extraction >> start_loading >> load_Dimcurrency >> finish_loading
