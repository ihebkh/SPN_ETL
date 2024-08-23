from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor
import os
import json
import psycopg2
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator




def Dimclients(**context):
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
    

    insert_client_query = """
    INSERT INTO dimclients (client_code, customer)
    VALUES (nextval('client_code_seq'), %s)
    ON CONFLICT (customer) DO NOTHING
    """

    client_name_to_pk = {}
    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for doc in requests_cleaned_data:
                customer_name = doc.get('customer')
                if customer_name:
                    cursor.execute(insert_client_query, (customer_name,))
            
            postgres_conn.commit()

            cursor.execute("SELECT client_pk, customer, client_code FROM dimclients;")
            clients_data = cursor.fetchall()
            for client_pk, customer, client_code in clients_data:
                client_name_to_pk[customer] = (client_pk, client_code)

    print("All client names and their corresponding 'client_pk' and 'client_code' in PostgreSQL:")
    for customer, (client_pk, client_code) in client_name_to_pk.items():
        print(f"{customer}: client_pk = {client_pk}, client_code = {client_code}")

    postgres_conn.close()
    print("Matching and insertion completed successfully.")

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
    'load_dag_dimclients',
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


   load_DimClients = PythonOperator(
        task_id='Dimclients',
        python_callable=Dimclients,
        provide_context=True,
        execution_timeout=timedelta(seconds=10),
        do_xcom_push=True
    )

wait_for_extraction >> start_loading >> load_DimClients >> finish_loading

