from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from airflow.utils import timezone
from datetime import datetime, timedelta, date
import time
import pendulum
from airflow.models import Variable
import requests as req
import pandas as pd
import psycopg2


def transform_flights(ti):
    
    data = ti.xcom_pull(task_ids = "extract_flights")
    data = data['states']
    columns = ['icao24', 'callsign', 
          'origin_country', 'time_position', 
          'last_contact', 'longitude', 
          'latitude', 'baro_altitude', 
          'on_ground', 'velocity', 
          'true_track', 'vertical_rate', 
          'sensors', 'geo_altitude', 
          'squawk', 'spi', 'category']
    df_flights = pd.DataFrame([data for data in data], columns = columns)
    df_flights['last_contact'] = df_flights['last_contact'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d'))
    
    
    df_flights.to_csv('/tmp/df_flights.csv', index = None, header = False)
    
    
def insert_in_table():
    hook = PostgresHook(postgres_conn_id = 'my-postgres')
    hook.copy_expert(sql = "COPY flights FROM stdin WITH DELIMITER ','",
                     filename = '/tmp/df_flights.csv')
    
def extract_transform_data():
    conn = psycopg2.connect(host='materials-postgres_2-1', user='natali', password='natali', dbname='mydb')
    df = pd.read_sql("SELECT * from flights", conn)
    df = df.groupby('last_contact').count().iloc[:,1]
    df.to_csv('/tmp/df_flights_ag.csv', header = False)
    
def insert_in_table_ag():
    hook = PostgresHook(postgres_conn_id = 'my-postgres')
    hook.copy_expert(sql = "COPY flights_ag FROM stdin WITH DELIMITER ','",
                     filename = '/tmp/df_flights_ag.csv')    

with DAG('flights', start_date = pendulum.datetime(2022, 11, 26, tz="Europe/Moscow"), schedule_interval = '@daily', catchup = True) as dag:
    
    create_table = PostgresOperator(    
             postgres_conn_id = 'my-postgres',
             task_id = 'create_table',
             sql = '''
                CREATE TABLE IF NOT EXISTS flights(
                icao24 TEXT NOT NULL, 
                callsign TEXT NOT NULL,
                origin_country TEXT NOT NULL,
                time_position int4, 
                last_contact TIMESTAMP,
                longitude float4, 
                latitude float4,
                baro_altitude text, 
                on_ground BOOLEAN, 
                velocity float4, 
                true_track float4,
                vertical_rate text, 
                sensors text, 
                geo_altitude text, 
                squawk TEXT NULL, 
                spi BOOLEAN, 
                category int4);''')
    
    create_table_ag = PostgresOperator(    
             postgres_conn_id = 'my-postgres',
             task_id = 'create_table_ag',
             sql = '''
                CREATE TABLE IF NOT EXISTS flights_ag(
                date_ex DATE NOT NULL, 
                count int4);''')
        
    is_api_available = HttpSensor(
               task_id = 'is_api_available',
               http_conn_id = 'flights_api',
               endpoint = 'api/states/all?lamin=45.8389&lomin=5.9962&lamax=47.8229&lomax=10.5226',
               request_params = {'begin': '{{ execution_date.timestamp() }}', 'end': '{{ execution_date.timestamp() }}'})
                
    extract_flights = SimpleHttpOperator(
               task_id = 'extract_flights',
               http_conn_id = 'flights_api',
               endpoint = 'api/states/all?lamin=45.8389&lomin=5.9962&lamax=47.8229&lomax=10.5226&begin={{ execution_date.timestamp() }}&end={{ execution_date.timestamp() }}',
               method = 'GET',
               response_filter = lambda response: response.json(),
               log_response = True)

    transform_flights = PythonOperator(
               task_id = 'transform_flights',
               python_callable = transform_flights) 
    
    insert_in_table = PythonOperator(
               task_id = 'insert_in_table',
               python_callable = insert_in_table) 
    
    extract_transform_data = PythonOperator(
               task_id = 'extract_transform_data',
               python_callable = extract_transform_data,
               trigger_rule='all_success'
               ) 
    
    insert_in_table_ag = PythonOperator(
               task_id = 'insert_in_table_ag',
               python_callable = insert_in_table_ag)

    
    
[create_table, create_table_ag] >> is_api_available >> extract_flights >> transform_flights >> insert_in_table >> extract_transform_data >> insert_in_table_ag
