import os
import json
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

requests_list_path = '/tmp/requests_list.json'
brands_cleaned_list_path = '/tmp/brands_cleaned.json'
list_of_cars_list_path = '/tmp/Listofcars.json'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def load_json_file(file_path):
    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"No data extracted or file does not exist at {file_path}")
    with open(file_path, 'r') as f:
        return json.load(f)

def FactTable(**context):
    requests_list_data = load_json_file(requests_list_path)
    brands_cleaned_data = load_json_file(brands_cleaned_list_path)

    postgres_conn_id = 'postgres'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_conn_str = f"dbname='{pg_conn.schema}' user='{pg_conn.login}' password='{pg_conn.password}' host='{pg_conn.host}' port='{pg_conn.port}'"
    postgres_conn = psycopg2.connect(pg_conn_str)

    def generate_unique_req_code(existing_codes):
        req_code = 1
        while req_code in existing_codes:
            req_code += 1
        return req_code

    client_name_to_pk = {}
    existing_req_codes = set()
    brand_to_car_pk = {}
    date_to_pk = {}
    requesttype_to_pk = {}
    slug_to_attributes = {}
    source_to_pk = {}
    adjustment_map = {}
    pays_to_region_pk = {}

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT client_pk, customer FROM dimclients;")
            clients_data = cursor.fetchall()
            for client_pk, customer in clients_data:
                client_name_to_pk[customer.strip()] = client_pk

            cursor.execute("SELECT req_code FROM factrequests;")
            req_codes_data = cursor.fetchall()
            for (req_code,) in req_codes_data:
                existing_req_codes.add(req_code)

            cursor.execute("SELECT car_pk, brand FROM dimcars;")
            cars_data = cursor.fetchall()
            for car_pk, brand in cars_data:
                normalized_brand = brand.replace(" ", "").lower()
                brand_to_car_pk[normalized_brand] = car_pk

            cursor.execute("SELECT date_pk, date FROM dimdates;")
            dates_data = cursor.fetchall()
            for date_pk, date in dates_data:
                date_to_pk[date.isoformat()] = date_pk

            cursor.execute("SELECT req_type_pk, reference, source FROM dimrequesttypes;")
            requesttypes_data = cursor.fetchall()
            for req_type_pk, reference, source in requesttypes_data:
                requesttype_to_pk[(reference.strip().lower(), source.strip().lower())] = req_type_pk

            cursor.execute("SELECT benchmark_pk, source FROM dimbenchmarks;")
            benchmarks_data = cursor.fetchall()
            for pk_dimbenchmarks, source in benchmarks_data:
                source_to_pk[source.strip().lower()] = pk_dimbenchmarks

            cursor.execute("SELECT offer_pk, adjustement_type, adjustement_unit FROM dimoffers;")
            offers_data = cursor.fetchall()
            for offer_pk, adjustement_type, adjustement_unit in offers_data:
                key = (adjustement_type.strip().lower(), adjustement_unit.strip().lower())
                adjustment_map[key] = offer_pk

            cursor.execute("SELECT pays, region_pk FROM dimregions;")
            regions_data = cursor.fetchall()
            for pays, region_pk in regions_data:
                pays_to_region_pk[pays.strip().lower()] = region_pk

    for doc in brands_cleaned_data:
        slug = doc.get('slug', '').strip().lower().replace(" ", "")
        big_luggage_count = doc.get('bigLuggageCount', None)
        small_luggage_count = doc.get('smallLuggageCount', None)
        passenger_count = doc.get('passengerCount', None)
        if slug:
            slug_to_attributes[slug] = {
                'big_luggage_count': big_luggage_count,
                'small_luggage_count': small_luggage_count,
                'passenger_count': passenger_count
            }

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for doc in requests_list_data:
                customer_name = doc.get('customer')
                if customer_name:
                    customer_name = customer_name.strip()
                    if customer_name in client_name_to_pk:
                        client_fk = client_name_to_pk[customer_name]
                        req_code = generate_unique_req_code(existing_req_codes)

                        existing_req_codes.add(req_code)

                        car_fk = None
                        date_fk = None
                        req_type_fk = None
                        region_fk = None
                        offer_fk = None
                        passenger_count_client = None
                        small_luggage_client = None
                        big_luggage_client = None
                        babies_count_client = None
                        adjustment_rate = None
                        offer_price = None
                        offer_quantity = None
                        total_price = None
                        nbr_benchmarks = 0
                        big_luggage_car = None
                        small_luggage_car = None
                        passenger_count_car = None
                        services = doc.get('services', [])
                        if services:
                            products = services[0].get('details', {}).get('products', [])
                            if products:
                                product = products[0].get('product', '').strip().lower().replace(" ", "")
                                if product in brand_to_car_pk:
                                    car_fk = brand_to_car_pk[product]

                                    if product in slug_to_attributes:
                                        attributes = slug_to_attributes[product]
                                        big_luggage_car = attributes.get('big_luggage_count')
                                        small_luggage_car = attributes.get('small_luggage_count')
                                        passenger_count_car = attributes.get('passenger_count')

                                passenger_count_client = products[0].get('passengersCount', None)

                            small_luggage_client = services[0].get('details', {}).get('smallLuggageCount', None)
                            big_luggage_client = services[0].get('details', {}).get('bigLuggageCount', None)
                            babies_count_client = services[0].get('details', {}).get('babiesCount', None)

                            offers = services[0].get('offers', [])
                            if offers:
                                adjustment_rate = offers[0].get('details', [{}])[0].get('adjustmentRate', {}).get('value', None)
                                adjustment_type = offers[0].get('details', [{}])[0].get('adjustmentRate', {}).get('type', '').strip().lower()
                                adjustment_unit = offers[0].get('details', [{}])[0].get('adjustmentRate', {}).get('unit', '').strip().lower()
                                if (adjustment_type, adjustment_unit) in adjustment_map:
                                    offer_fk = adjustment_map[(adjustment_type, adjustment_unit)]

                                offer_price = offers[0].get('details', [{}])[0].get('price', None)
                                offer_quantity = offers[0].get('details', [{}])[0].get('quantity', None)
                                total_price = offers[0].get('totalPrice', None)

                            benchmarks = services[0].get('benchmarks', [])
                            nbr_benchmarks = len(benchmarks)

                            bench_fk1 = None
                            bench_fk2 = None
                            bench_fk3 = None
                            bench_fk4 = None

                            if benchmarks:
                                if len(benchmarks) > 0:
                                    bench_fk1 = source_to_pk.get(benchmarks[0].get('source').strip().lower(), None)
                                if len(benchmarks) > 1:
                                    bench_fk2 = source_to_pk.get(benchmarks[1].get('source').strip().lower(), None)
                                if len(benchmarks) > 2:
                                    bench_fk3 = source_to_pk.get(benchmarks[2].get('source').strip().lower(), None)
                                if len(benchmarks) > 3:
                                    bench_fk4 = source_to_pk.get(benchmarks[3].get('source').strip().lower(), None)

                            pickup_date = services[0].get('details', {}).get('pickup', {}).get('date', {}).get('$date', '')
                            dropoff_date = services[0].get('details', {}).get('dropoff', {}).get('date', {}).get('$date', '')
                            if pickup_date:
                                pickup_date = pickup_date.split('T')[0]
                                if pickup_date in date_to_pk:
                                    date_fk = date_to_pk[pickup_date]
                            if dropoff_date:
                                dropoff_date = dropoff_date.split('T')[0]
                                if dropoff_date in date_to_pk:
                                    drop_off_date_fk = date_to_pk[dropoff_date]

                            location = services[0].get('details', {}).get('pickup', {}).get('location', {})
                            pick_up_place = location.get('city', '').strip()
                            dropoff_location = services[0].get('details', {}).get('dropoff', {}).get('location', {})
                            drop_off_place = dropoff_location.get('city', '').strip() if dropoff_location else None
                            country = location.get('country', '').strip().lower()
                            if country in pays_to_region_pk:
                                region_fk = pays_to_region_pk[country]

                            req_type_reference = doc.get('reference', '').strip().lower()
                            req_type_source = doc.get('source', '').strip().lower()
                            isb2b = doc.get('isB2B', False)
                            if (req_type_reference, req_type_source) in requesttype_to_pk:
                                req_type_fk = requesttype_to_pk[(req_type_reference, req_type_source)]

                        update_query = """
                        INSERT INTO factrequests (client_fk, req_code, car_fk, date_fk, req_type_fk, offer_fk, region_fk, drop_off_date, pick_up_date, passenger_count_client, small_luggage_client, big_luggage_client, babies_count_client, adjustement_rate, offer_price, offer_quantity, total_price, nbr_benchmarks, small_luggage_car, passenger_count_car, big_luggage_car, bench1_fk, bench2_fk, bench3_fk, bench4_fk, pick_up_place, drop_off_place)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (req_code) DO UPDATE SET 
                        client_fk = EXCLUDED.client_fk,
                        car_fk = EXCLUDED.car_fk, 
                        date_fk = EXCLUDED.date_fk, 
                        req_type_fk = EXCLUDED.req_type_fk, 
                        offer_fk = EXCLUDED.offer_fk,
                        region_fk = EXCLUDED.region_fk,
                        drop_off_date = EXCLUDED.drop_off_date, 
                        pick_up_date = EXCLUDED.pick_up_date, 
                        passenger_count_client = EXCLUDED.passenger_count_client, 
                        small_luggage_client = EXCLUDED.small_luggage_client, 
                        big_luggage_client = EXCLUDED.big_luggage_client, 
                        babies_count_client = EXCLUDED.babies_count_client,
                        adjustement_rate = EXCLUDED.adjustement_rate,
                        offer_price = EXCLUDED.offer_price,
                        offer_quantity = EXCLUDED.offer_quantity,
                        total_price = EXCLUDED.total_price,
                        nbr_benchmarks = EXCLUDED.nbr_benchmarks,
                        small_luggage_car = EXCLUDED.small_luggage_car,
                        passenger_count_car = EXCLUDED.passenger_count_car,
                        big_luggage_car = EXCLUDED.big_luggage_car,
                        bench1_fk = EXCLUDED.bench1_fk,
                        bench2_fk = EXCLUDED.bench2_fk,
                        bench3_fk = EXCLUDED.bench3_fk,
                        bench4_fk = EXCLUDED.bench4_fk,
                        pick_up_place = EXCLUDED.pick_up_place,
                        drop_off_place = EXCLUDED.drop_off_place
                        """
                        cursor.execute(update_query, (client_fk, req_code, car_fk, date_fk, req_type_fk, offer_fk, region_fk, dropoff_date or None, pickup_date or None, passenger_count_client, small_luggage_client, big_luggage_client, babies_count_client, adjustment_rate, offer_price, offer_quantity, total_price, nbr_benchmarks, small_luggage_car, passenger_count_car, big_luggage_car, bench_fk1, bench_fk2, bench_fk3, bench_fk4, pick_up_place, drop_off_place))

                    else:
                        print(f"No matching client found for customer {customer_name}")

            postgres_conn.commit()

def FactTable2(**context):
    list_of_cars_data = load_json_file(list_of_cars_list_path)

    postgres_conn_id = 'postgres'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_conn_str = f"dbname='{pg_conn.schema}' user='{pg_conn.login}' password='{pg_conn.password}' host='{pg_conn.host}' port='{pg_conn.port}'"
    postgres_conn = psycopg2.connect(pg_conn_str)

    plate_number_to_car_pk = {}
    car_fk_to_plate_number = {}

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT car_pk, plate_number FROM dimcars;")
            cars_data = cursor.fetchall()
            for car_pk, plate_number in cars_data:
                plate_number_to_car_pk[plate_number] = car_pk

            cursor.execute("SELECT car_fk FROM factrequests;")
            factrequests_data = cursor.fetchall()
            for (car_fk,) in factrequests_data:
                if car_fk in plate_number_to_car_pk.values():
                    for plate_number, car_pk in plate_number_to_car_pk.items():
                        if car_pk == car_fk:
                            car_fk_to_plate_number[car_fk] = plate_number
                            break

    def clean_and_convert_to_float(value):
        if value is None:
            return None
        if isinstance(value, str):
            value = value.split()[0]
        try:
            return float(value)
        except ValueError:
            return None

    with postgres_conn:
        with postgres_conn.cursor() as cursor:
            for doc in list_of_cars_data:
                plate_number = doc.get('Numéro de plaque', None)
                date_premiere_circulation = doc.get('Date 1er circulation', None)
                prix = clean_and_convert_to_float(doc.get('Prix', None))
                prix_annuel = clean_and_convert_to_float(doc.get('Prix Annuel', None))

                match_found = False
                for car_fk, original_plate_number in car_fk_to_plate_number.items():
                    if plate_number == original_plate_number:
                        update_query = """
                        UPDATE factrequests
                        SET date_circulation = %s,
                            car_price = %s,
                            prix_annuel = %s
                        WHERE car_fk = %s
                        """
                        cursor.execute(update_query, (date_premiere_circulation, prix, prix_annuel, car_fk))
                        match_found = True
                        break

            postgres_conn.commit()

    postgres_conn.close()

with DAG(
    'load_dag_fact',
    default_args=default_args,
    schedule_interval='*/4 * * * *',
    catchup=False
) as fact_dag:

    load_dag_fact1 = PythonOperator(
        task_id='load_dag_fact1',
        python_callable=FactTable,
        provide_context=True,
    )

    load_dag_fact2 = PythonOperator(
        task_id='load_dag_fact2',
        python_callable=FactTable2,
        provide_context=True,
    )

    finish_loading_fact = DummyOperator(task_id='finish_loading_fact')

    load_dag_fact1 >> load_dag_fact2 >> finish_loading_fact
