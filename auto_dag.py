from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum
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
dag1 = DAG('scrapy_auto_ru',
default_args=default_args,
description="education",
catchup=False,
schedule_interval='0 0 * * *')
task1 = BashOperator(
task_id='scrapy',
bash_command='python3 /home/anna/auto.py',
dag=dag)