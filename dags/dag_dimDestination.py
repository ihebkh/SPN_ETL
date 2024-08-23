from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import json
import psycopg2
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor

def load_dimdestinations(**context):
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

    def check_and_insert_destinations(postgres_conn, documents):
        insert_update_destination_query = """
        INSERT INTO dimdestinations (dest_code, longitude, latitude)
        VALUES (%s, %s, %s)
        ON CONFLICT (dest_code) DO UPDATE SET
        longitude = EXCLUDED.longitude,
        latitude = EXCLUDED.latitude
        """
        with postgres_conn:
            with postgres_conn.cursor() as cursor:
                for doc in documents:
                    services = doc.get('services', [])
                    for service in services:
                        details = service.get('details', {})
                        pickup = details.get('pickup', {})
                        location = pickup.get('location', {})
                        label = location.get('label', 'N/A')
                        if label != 'N/A':
                            dest_code = label.split(",")[-1].strip()
                        else:
                            dest_code = 'N/A'
                    
                        longitude = location.get('longitude')
                        latitude = location.get('latitude')
                    
                        if dest_code != 'N/A' and longitude is not None and latitude is not None:
                            print(f"Inserting/Updating: dest_code={dest_code}, longitude={longitude}, latitude={latitude}")
                            cursor.execute(insert_update_destination_query, (dest_code, longitude, latitude))
            
                postgres_conn.commit()

    check_and_insert_destinations(postgres_conn, requests_list_data)
    postgres_conn.close()
    print("Insertion/Update in dimdestinations completed successfully.")

def update_regionfk(**context):
    postgres_conn_id = 'postgres'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_conn_str = f"dbname='{pg_conn.schema}' user='{pg_conn.login}' password='{pg_conn.password}' host='{pg_conn.host}' port='{pg_conn.port}'"
    postgres_conn = psycopg2.connect(pg_conn_str)

    dest_code_to_pk = {}
    pays_to_region_pk = {}

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT dest_code, dest_pk FROM dimdestinations;")
            destinations_data = cursor.fetchall()
            for dest_code, dest_pk in destinations_data:
                dest_code_to_pk[dest_code] = dest_pk

            cursor.execute("SELECT pays, region_pk FROM dimregions;")
            regions_data = cursor.fetchall()
            for pays, region_pk in regions_data:
                pays_to_region_pk[pays] = region_pk

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for dest_code, dest_pk in dest_code_to_pk.items():
                region_pk = pays_to_region_pk.get(dest_code, None)
                if region_pk:
                    update_query = """
                    UPDATE dimdestinations
                    SET region_fk = %s
                    WHERE dest_pk = %s
                    """
                    cursor.execute(update_query, (region_pk, dest_pk))
                    print(f"Updated dest_pk={dest_pk} with region_pk={region_pk}")
                else:
                    print(f"No match found for dest_code={dest_code}")

    postgres_conn.commit()
    postgres_conn.close()
    print("Update of region_fk in dimdestinations completed successfully.")

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
    'load_dag_dimDestination',
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

    load_dimdestinations_task = PythonOperator(
        task_id='load_dimdestinations',
        python_callable=load_dimdestinations,
        provide_context=True,
        execution_timeout=timedelta(seconds=300),
        do_xcom_push=True
    )

    update_regionfk_task = PythonOperator(
        task_id='update_regionfk',
        python_callable=update_regionfk,
        provide_context=True,
        execution_timeout=timedelta(seconds=300),
        do_xcom_push=True
    )

    wait_for_extraction >> start_loading >> load_dimdestinations_task >> update_regionfk_task >> finish_loading
