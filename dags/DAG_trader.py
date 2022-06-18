# IMPORTANT - Import volume
import sys
sys.path.append(f"/opt/airflow/plugins/pipelines/ScalpFX/src/Utils_Python/folder_importer")
from FolderImporter import FolderImporter
FolderImporter()

# Import project folder
sys.path.append(f"/opt/airflow/plugins/pipelines/ScalpFX")
# import src.data_ingestion as data_ingestion

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
            description='Trade every 16 minutes from Monday to Saturday (Malaysia Time)',
            schedule_interval='16 * * * 1-6' # https://crontab.guru/
) as dag:
    pass