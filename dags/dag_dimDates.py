import json
import psycopg2
from psycopg2 import extras
import pandas as pd
from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

def create_date_df(start_date, end_date):
    date_range = pd.date_range(start_date, end_date)
    df = pd.DataFrame({
        'date': date_range,
        'Jour_Mois_Annee': date_range.strftime('%d-%m-%Y'),
        'Annee': date_range.year,
        'id_semestre': ((date_range.month - 1) // 6) + 1,
        'semestre': ((date_range.month - 1) // 6) + 1,
        'id_trimestre': ((date_range.month - 1) // 3) + 1,
        'trimestre': ((date_range.month - 1) // 3) + 1,
        'id_mois': date_range.month,
        'mois': date_range.month,
        'lib_mois': date_range.strftime('%B'),
        'id_jour': date_range.strftime('%A'),
        'jour': date_range.day,
        'lib_jour': date_range.strftime('%A'),
        'semaine': date_range.isocalendar().week,
        'JourDeAnnee': date_range.dayofyear,
        'Jour_mois_lettre': date_range.strftime('%d %B')
    })
    return df

def DimDates(**context):
   
    postgres_conn_id = 'postgres'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_conn_str = f"dbname='{pg_conn.schema}' user='{pg_conn.login}' password='{pg_conn.password}' host='{pg_conn.host}' port='{pg_conn.port}'"
    postgres_conn = psycopg2.connect(pg_conn_str)

    cursor = postgres_conn.cursor()

    start_date = '2022-01-01'
    end_date = '2024-12-31'
    date_df = create_date_df(start_date, end_date)

    data_tuples = [tuple(x) for x in date_df.to_numpy()]

    try:
        extras.execute_batch(cursor, """
            INSERT INTO dimDates (
                date, "Jour_Mois_Annee", "Annee", id_semestre, semestre, id_trimestre, trimestre,
                id_mois, mois, lib_mois, id_jour, jour, lib_jour, semaine, "JourDeAnnee", "Jour_mois_lettre"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) 
            DO UPDATE SET 
                "Jour_Mois_Annee" = EXCLUDED."Jour_Mois_Annee",
                "Annee" = EXCLUDED."Annee",
                id_semestre = EXCLUDED.id_semestre,
                semestre = EXCLUDED.semestre,
                id_trimestre = EXCLUDED.id_trimestre,
                trimestre = EXCLUDED.trimestre,
                id_mois = EXCLUDED.id_mois,
                mois = EXCLUDED.mois,
                lib_mois = EXCLUDED.lib_mois,
                id_jour = EXCLUDED.id_jour,
                jour = EXCLUDED.jour,
                lib_jour = EXCLUDED.lib_jour,
                semaine = EXCLUDED.semaine,
                "JourDeAnnee" = EXCLUDED."JourDeAnnee",
                "Jour_mois_lettre" = EXCLUDED."Jour_mois_lettre"
        """, data_tuples)
        postgres_conn.commit()
        print("Table dimDates alimentée avec succès!")
    except psycopg2.IntegrityError as e:
        print(f"Erreur d'intégrité lors de l'insertion des données : {e}")
        postgres_conn.rollback()
    except psycopg2.DataError as e:
        print(f"Erreur de données lors de l'insertion des données : {e}")
        postgres_conn.rollback()
    except psycopg2.DatabaseError as e:
        print(f"Erreur de base de données lors de l'insertion des données : {e}")
        postgres_conn.rollback()
    except Exception as e:
        print(f"Erreur générale lors de l'insertion des données : {e}")
        postgres_conn.rollback()
    finally:
        cursor.close()
        postgres_conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'load_dag_dimDates',
    default_args=default_args,
    description='A simple DAG to load dimension clients data',
    catchup=False,
    schedule_interval='*/3 * * * *',
) as dag:
    start_loading = DummyOperator(task_id='start_loading')
    finish_loading = DummyOperator(task_id='finish_loading')

    dim_dates_task = PythonOperator(
        task_id='dim_dates_task',
        python_callable=DimDates,
        provide_context=True,
        execution_timeout=timedelta(seconds=300),
        do_xcom_push=True
    )

    start_loading >> dim_dates_task >> finish_loading
