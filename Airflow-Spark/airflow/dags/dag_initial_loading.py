from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os

from functions import *

####################################
######## SPARK RUNNING ##########
####################################
os.environ["JAVA_HOME"] = "/opt/java"
os.environ["SPARK_HOME"] = "/opt/spark"

from pyspark.sql import SparkSession
import findspark
#from pyspark.context import SparkContext
#from pyspark.conf import SparkConf

#from pyspark.serializers import PickleSerializer

#conf = SparkConf()
#conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

#sc  = SparkContext(conf=conf, serializer=PickleSerializer())

findspark.init()
spark = SparkSession.builder.master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("OFF")
ks.set_option('compute.ops_on_diff_frames', True)

default_args = {
    'owner': 'Grupo 02 Henry Labs TF',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

path_base = '../../data/'


with DAG('InitialLoading100', schedule_interval='@once', default_args=default_args) as dag:
    StartPipeline = EmptyOperator(
        task_id = 'StartPipeline',
        dag = dag
        )

    PythonLoad1 = PythonOperator(
        task_id="LoadTips",
        python_callable=TipsEDA,
        )

    
    PythonLoad2 = PythonOperator(
        task_id="LoadCheckIns",
        python_callable=CheckinEDA,
        )

    PythonLoad3 = PythonOperator(
        task_id="LoadBusiness",
        python_callable=BusinessEDA,
        )

    PythonLoad4 = PythonOperator(
        task_id="LoadReviews",
        python_callable=ReviewEDA,
        )
    
    PythonLoad5 = PythonOperator(
        task_id="LoadUsers",
        python_callable=UserEDA,
        )

    # HalfETL= EmptyOperator(
    #     task_id = 'HalfETLAndLoading',
    #     dag = dag
    #     )
    
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


#StartPipeline >> PythonLoad1 >> PythonLoad2 >> PythonLoad3 >> PythonLoad4 >> PythonLoad5 >> FinishETL

StartPipeline >> [PythonLoad1, PythonLoad2] >> PythonLoad4 >> [PythonLoad3, PythonLoad5] >> FinishETL

#StartPipeline >> PythonLoad5 >> FinishETL

FinishETL >> CheckWithQuery >> FinishPipeline