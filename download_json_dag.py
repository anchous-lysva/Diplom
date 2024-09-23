from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import pandas as pd
from pandas.io import sql
from sqlalchemy import inspect,create_engine
from dateutil.relativedelta import relativedelta
import time
import requests
default_args = {
'owner': 'ARazepina',
'depends_on_past': False,
'start_date': pendulum.datetime(year=2024, month=6, day=1).in_timezone('Europe/Moscow'),
'email': ['razepina.annadm@gmail.com'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 0,
'retry_delay': timedelta(minutes=5)
}
dag = DAG('download_json',
default_args=default_args,
description="education",
catchup=False,
schedule_interval='0 0 * * *')
def download(**kwargs):
  encoding="ISO-8859-1"
  print('Hello from {kw}'.format(kw=kwargs['my_keyword']))
  df=pd.read_json('/home/anna/auto.json', orient='records')
  con=create_engine("mysql://Airflow:1@localhost:33061/spark")
  df.to_sql('auto',con,schema='spark',if_exists='append',index=False)  
task1 = PythonOperator(
    task_id='load',
    dag=dag,
    python_callable=download,
    op_kwargs={'my_keyword': 'Airflow 1234'}
)