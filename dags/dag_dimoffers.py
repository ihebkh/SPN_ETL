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


def Dimoffers(**context):
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

    insert_offer_query = """
    INSERT INTO dimoffers (offer_code, adjustement_type, adjustement_unit)
    VALUES (%s, %s, %s)
    ON CONFLICT (offer_code) 
    DO UPDATE SET 
        adjustement_type = EXCLUDED.adjustement_type,
        adjustement_unit = EXCLUDED.adjustement_unit;
    """

    documents = requests_list_data

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for doc in documents:
                reference = doc.get('reference')
                services = doc.get('services', [])
                
                for service in services:
                    offers = service.get('offers', [])
                    
                    for offer in offers:
                        details = offer.get('details', [])
                        
                        for detail in details:
                            adjustment = detail.get('adjustmentRate', {})
                            
                            adjustement_type = adjustment.get('type')
                            adjustement_unit = adjustment.get('unit')
                        
                            
                            if adjustement_type and adjustement_unit is not None:
                                cursor.execute(insert_offer_query, (reference, adjustement_type, adjustement_unit))

        



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
    'load_dag_dimoffers',
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
    

     load_Dimoffers = PythonOperator(
        task_id='Dimoffers',
        python_callable=Dimoffers,
        provide_context=True,
        execution_timeout=timedelta(seconds=300),
        do_xcom_push=True
    )
     wait_for_extraction >> start_loading >> load_Dimoffers >> finish_loading

    
