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

plate_number_counter = 1

def DimCars(**context):
    Listofcars_list_path = '/tmp/Listofcars.json'
    cars_cleaned_list_path = '/tmp/cars_cleaned.json'
    brands_cleaned_list_path = '/tmp/brands_cleaned.json'

    def load_json_file(file_path):
        if not file_path or not os.path.exists(file_path):
            raise ValueError(f"No data extracted or file does not exist at {file_path}")
        with open(file_path, 'r') as f:
            return json.load(f)

    list_of_cars_data = load_json_file(Listofcars_list_path)
    cars_cleaned_data = load_json_file(cars_cleaned_list_path)
    brands_cleaned_data = load_json_file(brands_cleaned_list_path)

    postgres_conn_id = 'postgres'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_conn_str = f"dbname='{pg_conn.schema}' user='{pg_conn.login}' password='{pg_conn.password}' host='{pg_conn.host}' port='{pg_conn.port}'"
    postgres_conn = psycopg2.connect(pg_conn_str)

    plate_number_to_nom = {}
    for doc in list_of_cars_data:
        plate_number = doc.get('Numéro de plaque')
        nom = doc.get('Nom')
        if plate_number and nom:
            plate_number_to_nom[plate_number] = nom

    slug_energy_map = {}
    slug_subtype_map = {}
    for doc in brands_cleaned_data:
        slug = doc.get('slug')
        energy = doc.get('energy')
        subtype = doc.get('subtype')
        if slug and energy:
            slug_energy_map[slug] = energy
        if slug and subtype:
            slug_subtype_map[slug] = subtype

    upsert_car_query = """
    INSERT INTO dimcars (plate_number, brand, slug, isactive, isavailable, ispro, owner, image, energy, sub_type, assurance_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (plate_number) 
    DO UPDATE SET 
        brand = EXCLUDED.brand,
        slug = EXCLUDED.slug,
        isactive = EXCLUDED.isactive,
        isavailable = EXCLUDED.isavailable,
        ispro = EXCLUDED.ispro,
        owner = EXCLUDED.owner,
        image = EXCLUDED.image,
        energy = EXCLUDED.energy,
        sub_type = EXCLUDED.sub_type,
        assurance_name = EXCLUDED.assurance_name
    """

    documents1 = cars_cleaned_data

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for doc in documents1:
                plate_number = doc.get('plateNumber')
                brand = doc.get('brand')
                slug = doc.get('slug')
                isActive = doc.get('isActive', False)
                isAvailable = doc.get('isAvailable', False)
                isPro = doc.get('isPro', False)
                owner = doc.get('owner', '')
                image_url = doc.get('image', {}).get('url', '')
                energy = slug_energy_map.get(brand, '')
                subtype = slug_subtype_map.get(brand, '')
                assurance_name = plate_number_to_nom.get(plate_number, '')

                if plate_number:
                    cursor.execute(upsert_car_query, (plate_number, brand, slug, isActive, isAvailable, isPro, owner, image_url, energy, subtype, assurance_name))
        
            postgres_conn.commit()

    def generate_plate_number():
        global plate_number_counter
        plate_number = f"GE {plate_number_counter:06d}"
        plate_number_counter += 1
        return plate_number

    def brand_exists(cursor, brand):
        cursor.execute("SELECT 1 FROM dimcars WHERE brand = %s", (brand,))
        return cursor.fetchone() is not None

    upsert_additional_car_query = """
    INSERT INTO dimcars (plate_number, brand, slug, isactive, image)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (plate_number) 
    DO UPDATE SET 
        brand = EXCLUDED.brand,
        slug = EXCLUDED.slug,
        isactive = EXCLUDED.isactive,
        image = EXCLUDED.image
    """

    documents2 = brands_cleaned_data

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for doc in documents2:
                brand = doc.get('slug', 'default-brand').lower()
                isActive = doc.get('isActive', False)
                plate_number = generate_plate_number()  
                image = doc.get('image', {}).get('url', 'None')

                if not brand_exists(cursor, brand):
                    modified_slug = f"{brand}-{plate_number.lower().replace(' ', '-')}" 
                    cursor.execute(upsert_additional_car_query, (plate_number, brand, modified_slug, isActive, image))
        
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
    'load_dag_dimCars',
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
 
    load_DimCars = PythonOperator(
        task_id='DimCars',
        python_callable=DimCars,
        provide_context=True,
        execution_timeout=timedelta(seconds=10),
        do_xcom_push=True
    )
    wait_for_extraction >> start_loading >> load_DimCars >> finish_loading
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

plate_number_counter = 1

def DimCars(**context):
    Listofcars_list_path = '/tmp/Listofcars.json'
    cars_cleaned_list_path = '/tmp/cars_cleaned.json'
    brands_cleaned_list_path = '/tmp/brands_cleaned.json'

    def load_json_file(file_path):
        if not file_path or not os.path.exists(file_path):
            raise ValueError(f"No data extracted or file does not exist at {file_path}")
        with open(file_path, 'r') as f:
            return json.load(f)

    list_of_cars_data = load_json_file(Listofcars_list_path)
    cars_cleaned_data = load_json_file(cars_cleaned_list_path)
    brands_cleaned_data = load_json_file(brands_cleaned_list_path)

    postgres_conn_id = 'postgres'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_conn_str = f"dbname='{pg_conn.schema}' user='{pg_conn.login}' password='{pg_conn.password}' host='{pg_conn.host}' port='{pg_conn.port}'"
    postgres_conn = psycopg2.connect(pg_conn_str)

    plate_number_to_nom = {}
    for doc in list_of_cars_data:
        plate_number = doc.get('Numéro de plaque')
        nom = doc.get('Nom')
        if plate_number and nom:
            plate_number_to_nom[plate_number] = nom

    slug_energy_map = {}
    slug_subtype_map = {}
    for doc in brands_cleaned_data:
        slug = doc.get('slug')
        energy = doc.get('energy')
        subtype = doc.get('subtype')
        if slug and energy:
            slug_energy_map[slug] = energy
        if slug and subtype:
            slug_subtype_map[slug] = subtype

    upsert_car_query = """
    INSERT INTO dimcars (plate_number, brand, slug, isactive, isavailable, ispro, owner, image, energy, sub_type, assurance_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (plate_number) 
    DO UPDATE SET 
        brand = EXCLUDED.brand,
        slug = EXCLUDED.slug,
        isactive = EXCLUDED.isactive,
        isavailable = EXCLUDED.isavailable,
        ispro = EXCLUDED.ispro,
        owner = EXCLUDED.owner,
        image = EXCLUDED.image,
        energy = EXCLUDED.energy,
        sub_type = EXCLUDED.sub_type,
        assurance_name = EXCLUDED.assurance_name
    """

    documents1 = cars_cleaned_data

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for doc in documents1:
                plate_number = doc.get('plateNumber')
                brand = doc.get('brand')
                slug = doc.get('slug')
                isActive = doc.get('isActive', False)
                isAvailable = doc.get('isAvailable', False)
                isPro = doc.get('isPro', False)
                owner = doc.get('owner', '')
                image_url = doc.get('image', {}).get('url', '')
                energy = slug_energy_map.get(brand, '')
                subtype = slug_subtype_map.get(brand, '')
                assurance_name = plate_number_to_nom.get(plate_number, '')

                if plate_number:
                    cursor.execute(upsert_car_query, (plate_number, brand, slug, isActive, isAvailable, isPro, owner, image_url, energy, subtype, assurance_name))
        
            postgres_conn.commit()

    def generate_plate_number():
        global plate_number_counter
        plate_number = f"GE {plate_number_counter:06d}"
        plate_number_counter += 1
        return plate_number

    def brand_exists(cursor, brand):
        cursor.execute("SELECT 1 FROM dimcars WHERE brand = %s", (brand,))
        return cursor.fetchone() is not None

    upsert_additional_car_query = """
    INSERT INTO dimcars (plate_number, brand, slug, isactive, image)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (plate_number) 
    DO UPDATE SET 
        brand = EXCLUDED.brand,
        slug = EXCLUDED.slug,
        isactive = EXCLUDED.isactive,
        image = EXCLUDED.image
    """

    documents2 = brands_cleaned_data

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for doc in documents2:
                brand = doc.get('slug', 'default-brand').lower()
                isActive = doc.get('isActive', False)
                plate_number = generate_plate_number()  
                image = doc.get('image', {}).get('url', 'None')

                if not brand_exists(cursor, brand):
                    modified_slug = f"{brand}-{plate_number.lower().replace(' ', '-')}" 
                    cursor.execute(upsert_additional_car_query, (plate_number, brand, modified_slug, isActive, image))
        
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
    'load_dag_dimCars',
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
 
    load_DimCars = PythonOperator(
        task_id='DimCars',
        python_callable=DimCars,
        provide_context=True,
        execution_timeout=timedelta(seconds=10),
        do_xcom_push=True
    )
    wait_for_extraction >> start_loading >> load_DimCars >> finish_loading
