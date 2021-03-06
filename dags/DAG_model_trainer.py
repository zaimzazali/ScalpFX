# IMPORTANT - Import volume
from Utils_Python.folder_importer.FolderImporter import FolderImporter
FolderImporter()
# import ScalpFX.src.data_ingestion as data_ingestion

from datetime import timedelta, datetime
import pytz

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

DAG_ID = 'MODEL_TRAINER'

default_args = {
    'owner': 'Zaim Zazali',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1, tzinfo=pytz.timezone('Asia/Kuala_Lumpur')),
    # 'end_date': datetime(2022, 12, 31, tzinfo=pytz.timezone('Asia/Kuala_Lumpur')),
    'email': ['zaim.zazali@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(DAG_ID,
            default_args=default_args,
            description='Train model every 2 days at 8.00pm',
            schedule_interval='0 20 */2 * *' # https://crontab.guru/
) as dag:
    pass