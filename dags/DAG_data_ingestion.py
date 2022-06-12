import sys
from datetime import timedelta, datetime
import pytz

# IMPORTANT - Import pipelines volumes
sys.path.append(f"/opt/airflow/pipelines")

import ScalpFX.src.data_ingestion as data_ingestion

from airflow import DAG
from airflow.operators.python_operator import PythonOperator



default_args = {
    'owner': 'Zaim Zazali',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1, tzinfo=pytz.timezone('Asia/Kuala_Lumpur')),
    # 'end_date': datetime(2022, 12, 31, tzinfo=pytz.timezone('Asia/Kuala_Lumpur')),
    'email': ['zaim.zazali@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG('DATA_INGESTION_GBPUSD_15MIN',
            default_args=default_args,
            description='Ingest hourly data up to current timestamp.',
            schedule_interval='@hourly',
            catchup=False
) as dag:
    data = data_ingestion.getData()

    # region- Process Nodes
    t0 = PythonOperator(
        task_id='Get_Latest_Timestamp',
        python_callable=data_ingestion.getLatestTimestamp,
        op_kwargs={'dbConfig':data['dbConfig']},
        provide_context=True
    )

    # t1 = PythonOperator(
    #     task_id='Get_Latest_Timestamp',
    #     python_callable=data_ingestion.getHistoricalData,
    #     op_kwargs={'dbConfig':data['dbConfig']},
    #     provide_context=True
    # )




    # t1 = PythonOperator(
    #     task_id='ingest_data',
    #     python_callable=daily_load.ingestData,
    #     op_kwargs={'targetTables':data['targetTables'], 'sourceTables':data['sourceTables'], 
    #                 'targetDB':data['targetDB'], 'schemaName':data['schemaName'], 
    #                 'plazaGroupFilePath':plazaGroupFilePath},
    #     provide_context=True
    # )

    # t2 = PythonOperator(
    #     task_id='create_indexes',
    #     python_callable=daily_load.createIndexes,
    #     op_kwargs={'targetDB':data['targetDB'], 'targetTables':data['targetTables']},
    #     provide_context=True
    # )
    # endregion

    # DAG Pipeline
    t0 
