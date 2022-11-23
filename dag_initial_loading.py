from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from functions import *

default_args = {
    'owner': 'Grupo 02 Henry Labs TF',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

path_base = './data/'
path_precio = './data/new_reviews/'


with DAG('InitialLoading', schedule_interval='@once', default_args=default_args) as dag:
    StartPipeline = EmptyOperator(
        task_id = 'StartPipeline',
        dag = dag
        )

    PythonLoad1 = PythonOperator(
        task_id="LoadTips",
        python_callable=TipsEDA,
        )

    PythonLoad2 = PythonOperator(
        task_id="LoadReviews",
        python_callable=ReviewEDA,
        )

    PythonLoad3 = PythonOperator(
        task_id="LoadUsers",
        python_callable=UserEDA,
        )
    
    PythonLoad4 = PythonOperator(
        task_id="LoadCheckIns",
        python_callable=CheckinEDA,
        )

    PythonLoad5 = PythonOperator(
    task_id="LoadBusiness",
    python_callable=BusinessEDA,
    )

    FinishETL= EmptyOperator(
    task_id = 'FinishETLAndLoading',
    dag = dag
    )

    CheckWithQuery = PythonOperator(
        task_id="CheckWithQuery",
        python_callable=MakeQuery,
        op_kwargs={
        'query': 'SELECT * FROM reviews LIMIT 10',
        }
)

    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


StartPipeline >> [PythonLoad1, PythonLoad2, PythonLoad3] >> FinishETL

FinishETL >>  CheckWithQuery >> FinishPipeline