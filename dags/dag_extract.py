from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from bson import ObjectId
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
from datetime import datetime, timedelta
from bson import ObjectId
from airflow.operators.dummy import DummyOperator

def serialize_data(data):
    if isinstance(data, list):
        return [serialize_data(item) for item in data]
    elif isinstance(data, dict):
        return {key: serialize_data(value) for key, value in data.items()}
    elif isinstance(data, datetime):
        return data.isoformat() 
    elif isinstance(data, ObjectId):
        return str(data) 
    else:
        return data

def extract(**context):
    def convert_objectid_to_str(doc):
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                doc[key] = str(value)
        return doc

    mongo_conn_id = 'mongocnx'
    mongo_hook = MongoHook(conn_id=mongo_conn_id)
    
    requests = mongo_hook.get_collection('requests_cleaned', 'SA_SPN')
    requests_list = [convert_objectid_to_str(doc) for doc in requests.find({})]
    
    Listofcars = mongo_hook.get_collection('list_of_cars', 'SA_SPN')
    Listofcars_list = [convert_objectid_to_str(doc) for doc in Listofcars.find({})]
    
    cars_cleaned = mongo_hook.get_collection('Cars_cleaned', 'SA_SPN')
    cars_cleaned_list = [convert_objectid_to_str(doc) for doc in cars_cleaned.find({})]
    
    brands_cleaned = mongo_hook.get_collection('brands_cleaned', 'SA_SPN')
    brands_cleaned_list = [convert_objectid_to_str(doc) for doc in brands_cleaned.find({})]
    
    region_SA = mongo_hook.get_collection('region_SA', 'SA_SPN')
    region_SA_list = [convert_objectid_to_str(doc) for doc in region_SA.find({})]


    
    serialized_data_requests = serialize_data(requests_list)
    serialized_data_Listofcars_list = serialize_data(Listofcars_list)
    serialized_data_cars_cleaned_list = serialize_data(cars_cleaned_list)
    serialized_data_brands_cleaned_list = serialize_data(brands_cleaned_list)

    serialized_data_region_SA_list = serialize_data(region_SA_list)


    requests_list_path = '/tmp/requests_list.json'
    Listofcars_list_path = '/tmp/Listofcars.json'
    cars_cleaned_list_path = '/tmp/cars_cleaned.json'
    brands_cleaned_list_path = '/tmp/brands_cleaned.json'
    region_SA_list_path = '/tmp/region_SA.json'




    with open(requests_list_path, 'w') as f1:
        json.dump(serialized_data_requests, f1)
    with open(Listofcars_list_path, 'w') as f2:
        json.dump(serialized_data_Listofcars_list, f2)
    with open(cars_cleaned_list_path, 'w') as f3:
        json.dump(serialized_data_cars_cleaned_list, f3)
    with open(brands_cleaned_list_path, 'w') as f4:
        json.dump(serialized_data_brands_cleaned_list, f4)
    with open(region_SA_list_path, 'w') as f5:
        json.dump(serialized_data_region_SA_list, f5)        
    
    

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 20, 0, 0), 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'extract_dag',
    schedule_interval='*/3 * * * *', 
    default_args=default_args,
    description='Extract data from MongoDB',
    catchup=False,
) as dag:
    
    start_extraction = DummyOperator(task_id='start_extraction')

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        dag=dag,
        provide_context=True,

)
    finish_extraction = DummyOperator(task_id='finish_extraction')
    start_extraction >> extract_task >> finish_extraction


