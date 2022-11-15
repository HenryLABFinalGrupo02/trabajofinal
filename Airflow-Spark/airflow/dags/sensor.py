from datetime import datetime, timedelta
#from sys import get_asyncgen_hooks
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.taskinstance import TaskInstance as ti
from tempfile import NamedTemporaryFile
import os
import pathlib
import pandas as pd

import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.sql.functions import isnan, when, count, col

os.environ["JAVA_HOME"] = "/opt/java"
os.environ["SPARK_HOME"] = "/opt/spark"

findspark.init("/opt/spark")


#Check version of Airflow and Pip Amazon installed 
#Sensor to check if the file is loaded in the bucket
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

#Hook to connect to S3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#from functions import *
#from etl import *

#old_files = GetFiles()

#Set path for new files
dest_file_path = '/opt/airflow/data/to_transform/'
dest_file_path_clean = '/opt/airflow/data/transformed/'

def DownloadAndRenameFile(bucket_name:str, path:str):
    hook = S3Hook('minio_conn')
    files = hook.list_keys(bucket_name=bucket_name)
    key = files[-1]
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=path)
    RenameFile(file_name, key)
    return file_name

def RenameFile(file_name:str, new_name:str) -> None:
    downloaded_file_path = '/'.join(file_name.split('/')[:-1])
    os.rename(src=file_name, dst=f'{downloaded_file_path}/{new_name}')
    print('Renamed successfully')


def ListarArchivos(): #Funcion para listar los archivos de Precios
    archivos_precio = []
    c = pathlib.Path(r'/opt/airflow/data/to_transform/')
    
    for entrada in c.iterdir():
        if entrada.is_file():
            archivos_precio.append(entrada)
    
    archivos_precio.sort()
    print(pd.read_csv(archivos_precio[0]))
    print(str(archivos_precio[0]),'hola')
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.load(str(archivos_precio[0]), format="csv")
    
    print('hola2')
    print(df.show(truncate=False))
    print('hola3')



# Default arguments
default_args = {
    'owner': 'Maico Bernal',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

# DAG
with DAG(
    dag_id='DAG_Minio_S3_Wait_for_File',
    start_date=datetime(2022, 11, 10),
    schedule_interval='@daily',
    default_args=default_args
) as dag:

    CheckS3 = S3KeySensor(

        #https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/s3/index.html
        task_id='S3BucketSensorNewFiles', #Name of task
        bucket_name='data', #Support relative or full path
        bucket_key='precio*', #Only if we didn't specify the full path, or we want to use UNIx style wildcards
        wildcard_match = True, #Set to true if we want to use wildcards
        aws_conn_id='minio_conn', #Name of the connection
        mode='poke', #Poke or reschedule
        poke_interval=5,
        timeout=30
    )

    #Download the file from S3/Minio
    DownloadFileFromS3 = PythonOperator(
        task_id='DownloadFileFromS3',
        python_callable=DownloadAndRenameFile,
        op_kwargs={
            'bucket_name': 'data',
            'path': dest_file_path,
            }
    )

    FinishDownload = EmptyOperator(
        task_id='FinishDownload'
    )

    #Check for new files and load them
    PythonAndSQLLoad = PythonOperator(
        task_id="LoadNewPrices",
        python_callable=ListarArchivos
        #op_kwargs={'path_new': dest_file_path}
        )

    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


CheckS3 >> DownloadFileFromS3 >> FinishDownload
FinishDownload >> PythonAndSQLLoad >> FinishPipeline