from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import os
import json
import psycopg2
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

def dimBenchmark(**context):
    requests_list_path = '/tmp/requests_list.json'

    def load_json_file(file_path):
        if not file_path or not os.path.exists(file_path):
            raise ValueError(f"No data extracted or file does not exist at {file_path}")
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error decoding JSON from file {file_path}: {e}")

    requests_list_data = load_json_file(requests_list_path)

    postgres_conn_id = 'postgres'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_conn_str = f"dbname='{pg_conn.schema}' user='{pg_conn.login}' password='{pg_conn.password}' host='{pg_conn.host}' port='{pg_conn.port}'"
    postgres_conn = psycopg2.connect(pg_conn_str)

    code_to_currency_pk = {}

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT currency_pk, code FROM dimcurrency;")
            currencies_data = cursor.fetchall()
            for currency_pk, code in currencies_data:
                code_to_currency_pk[code.strip().upper()] = currency_pk  # Normalize to uppercase and strip spaces

            insert_benchmark_query = """
            INSERT INTO dimBenchmarks (benchmark_code, source, price, currency_fk)
            VALUES (nextval('benchmark_code_seq'), %s, %s, %s)
            ON CONFLICT (benchmark_code) DO UPDATE SET
            currency_fk = EXCLUDED.currency_fk,
            price = EXCLUDED.price
            """

            for doc in requests_list_data:
                services = doc.get('services', [])
                for service in services:
                    benchmarks = service.get('benchmarks', [])
                    for benchmark in benchmarks:
                        source = benchmark.get('source', '')
                        details = benchmark.get('details', [])
                        currency_fk = None
                        price = None
                        for detail in details:
                            currency = detail.get('currency', {})
                            code = currency.get('code', '').strip().upper() if currency.get('code') else ''
                            if code in code_to_currency_pk:
                                currency_fk = code_to_currency_pk[code]
                                price = detail.get('price', None)
                                break

                        if source and currency_fk and price is not None:
                            cursor.execute(insert_benchmark_query, (source, price, currency_fk))

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
    'load_dag_dimBenchmark',
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

    dimBenchmark_task = PythonOperator(
        task_id='dimBenchmark',
        python_callable=dimBenchmark,
        provide_context=True,
        execution_timeout=timedelta(seconds=10),
        do_xcom_push=True
    )

    wait_for_extraction >> start_loading >> dimBenchmark_task >> finish_loading
