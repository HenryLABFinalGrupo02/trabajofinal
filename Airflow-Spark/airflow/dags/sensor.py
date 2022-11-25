from datetime import datetime, timedelta
#from sys import get_asyncgen_hooks
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.taskinstance import TaskInstance as ti
from tempfile import NamedTemporaryFile
import os


#Check version of Airflow and Pip Amazon installed 
#Sensor to check if the file is loaded in the bucket
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

#Hook to connect to S3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


#Set path for new files
dest_file_path = '/opt/data/minio/'
dest_file_path_clean = '/opt/data/minio/cleaned/'

# Default arguments
default_args = {
    'owner': 'DS04_TF_G2_Henry',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

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

def load_review_new():
    import casspark
    from cassandra.cluster import Cluster
    cass_ip = 'cassandra'
    import transform_funcs
    import pyspark.pandas as ps
    print('ESTABLISHING CONNECTION TO CASSANDRA')
    cluster = Cluster(contact_points=[cass_ip],port=9042)
    session = cluster.connect()

    import glob
    path = dest_file_path
        #Get all files in the folder
    try:
        all_json = glob.glob(path + "/*.json")

        if len(all_json) == 0:
            raise FileNotFoundError('No files found in the folder')
    except:
        print('Error with path or files GLOB ERROR')

    #Get all JSON in the folder
    if len(all_json) > 0:
        for filename in all_json:
            print(f'READING REVIEW FILE {filename}')
            review = ps.read_json(filename, lines=True)
            print('DROPPING DUPLICATED ROWS')
            review = review.drop_duplicates()
            print('NORMALIZING DATES')
            review['date'] = review['date'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')
            print('CREATING KEYSPACE')
            session.execute("""
            CREATE KEYSPACE IF NOT EXISTS henry WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
            """)
            print('CREATING TABLE')
            session.execute("""
            CREATE TABLE IF NOT EXISTS henry.review(review_id text, user_id text business_id text stars float date string text string useful int funny int cool int,PRIMARY KEY(review_id))
            """)
            print('UPLOADING DATAFRAME TO CASSANDRA')
            casspark.spark_pandas_insert(review,'henry','review',session,debug=True)
            print('DONE')

        print('All JSON files imported and cleaned successfully')
    else:
        print('No JSON files found')
    
def MakeQuery():
    print('QUERY')
    return 'Query done successfully'


# DAG
with DAG(
    dag_id='DAG_Minio_S3_Wait_for_File',
    start_date=datetime(2022, 11, 22),
    schedule_interval='@daily',
    default_args=default_args
) as dag:

    CheckS3 = S3KeySensor(

        #https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/s3/index.html
        task_id='S3BucketSensorNewFiles', #Name of task
        bucket_name='data', #Support relative or full path
        bucket_key='review*', #Only if we didn't specify the full path, or we want to use UNIx style wildcards
        wildcard_match = True, #Set to true if we want to use wildcards
        aws_conn_id='minio_conn', #Name of the connection
        mode='poke', #Poke or reschedule
        poke_interval=5,
        timeout=120
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
    load_new_reviews = PythonOperator(
        task_id="load_review_new",
        python_callable=load_review_new
        )

    #Make a Query to check if everything is fine
    CheckNewPricesQuery = PythonOperator(
        task_id="CheckNewPricesQuery",
        python_callable=MakeQuery,
        )

    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


CheckS3 >> DownloadFileFromS3 >> FinishDownload
FinishDownload >> load_new_reviews >> CheckNewPricesQuery >> FinishPipeline