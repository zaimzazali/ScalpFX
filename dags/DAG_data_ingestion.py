# IMPORTANT - Import volume
from Utils_Python.folder_importer.FolderImporter import FolderImporter
FolderImporter()
import ScalpFX.src.data_ingestion as data_ingestion

from datetime import timedelta, datetime
import pytz

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

DAG_ID = 'DATA_INGESTION_GBPUSD_15MIN'

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
            description='Ingest hourly data up to current timestamp.',
            schedule_interval='1 * * * *' # https://crontab.guru/
) as dag:
    t0 = PythonOperator(
        task_id=f"{DAG_ID}_Get_Data",
        python_callable=data_ingestion.getData,
        op_kwargs={'connTag':"PostgresqlIgTrading",
                   'loginType':"live"},
        provide_context=True
    )
    
    t1 = PythonOperator(
        task_id=f"{DAG_ID}_Get_Latest_Timestamp",
        python_callable=data_ingestion.getLatestTimestamp,
        op_kwargs={'taskIDs':{'getData':f"{DAG_ID}_Get_Data"}},
        provide_context=True
    )

    t2 = PythonOperator(
        task_id=f"{DAG_ID}_Get_Historical_Data",
        python_callable=data_ingestion.getHistoricalData,
        op_kwargs={'taskIDs':{
                                'getData':f"{DAG_ID}_Get_Data", 
                                'getLatestTimestamp':f"{DAG_ID}_Get_Latest_Timestamp"}},
        provide_context=True
    )

    t3 = PythonOperator(
        task_id=f"{DAG_ID}_Calculate_Mid_Values",
        python_callable=data_ingestion.calculateMidValues,
        op_kwargs={'taskIDs':{'getHistoricalData':f"{DAG_ID}_Get_Historical_Data"}},
        provide_context=True
    )

    t4 = PythonOperator(
        task_id=f"{DAG_ID}_Delete_Dirty_Data",
        python_callable=data_ingestion.deleteDirtyData,
        op_kwargs={'taskIDs':{
                                'getData':f"{DAG_ID}_Get_Data",
                                'getLatestTimestamp':f"{DAG_ID}_Get_Latest_Timestamp"}},
        provide_context=True
    )

    t5 = PythonOperator(
        task_id=f"{DAG_ID}_Push_Data_To_Database",
        python_callable=data_ingestion.pushDataToDatabase,
        op_kwargs={'taskIDs':{
                                'getData':f"{DAG_ID}_Get_Data",
                                'calculateMidValues':f"{DAG_ID}_Calculate_Mid_Values"}},
        provide_context=True
    )

    # DAG Pipeline
    t0 >> t1 >> t2 >> t3 >> t4 >> t5
