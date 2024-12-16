from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from monokera.monokera_report import MonokeraReport
import os

DAG_NAME = 'Ensurance_Monokera'


POSTGRESQL_CONN = {}
POSTGRESQL_CONN['POSTGRESQL_HOST'] = '0.0.0.0'  #os.getenv("POSTGRES_HOST")
POSTGRESQL_CONN['POSTGRESQL_USER'] = 'admin'      #os.getenv("POSTGRES_USER")
POSTGRESQL_CONN['POSTGRESQL_PASSWORD'] = 'root'   #os.getenv("POSTGRES_PASSWORD")
POSTGRESQL_CONN['POSTGRESQL_DB'] = 'monokera'     #os.getenv("POSTGRES_DB")
POSTGRESQL_CONN['POSTGRESQL_PORT'] = 5433         #os.getenv("POSTGRES_PORT")
POSTGRESQL_CONN['POSTGRESQL_SCHEMA'] = 'monokera' #os.getenv("POSTGRES_SCHEMA")

SFTP_CONN = {}
SFTP_CONN['SFTP_HOST'] = '0.0.0.0'
SFTP_CONN['SFTP_USER'] = 'foo'
SFTP_CONN['SFTP_PASSWORD'] = 'pass'
SFTP_CONN['LOCAL_FILE'] = 'upload/'
SFTP_CONN['FILENAME'] = 'MOCK_DATA.csv'

default_args = {
                'owner': 'daniel nieto',
                'depends_on_past': False,
                'email': ['nietopadilla@gmail.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=1)
                }

dag = DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2024, 12, 16),
    schedule_interval=None,
    default_args=default_args,
    catchup=False
)
start_dummy = DummyOperator(
    task_id='start_task',
    dag=dag
)
end_dummy = DummyOperator(
    task_id='end_task',
    dag=dag
)

processor = PythonOperator(
    task_id='processor_task',
    python_callable=MonokeraReport().run,
    op_kwargs= {'POSTGRESQL_CONN':POSTGRESQL_CONN,'SFTP_CONN':SFTP_CONN},
    dag=dag
)

start_dummy>>processor>>end_dummy
