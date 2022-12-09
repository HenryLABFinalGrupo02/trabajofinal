from datetime import datetime, timedelta
#from sys import get_asyncgen_hooks
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.taskinstance import TaskInstance as ti
from tempfile import NamedTemporaryFile
import os
import sqlalchemy
os.environ["JAVA_HOME"] = "/opt/java"
os.environ["SPARK_HOME"] = "/opt/spark"
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
import pyspark.pandas as ps
from transform_funcs import lower_col_names,transform_dates, drop_bad_str


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

def lower_col_names(cols):
    new_names = {}
    for x in cols:
        new_names[x] = x.lower()
    return new_names    

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

def LoadNewReviewsOrTips():
    # import casspark
    # from cassandra.cluster import Cluster
    # cass_ip = 'cassandra'
    # import pyspark as ps
    import pandas as pd
    # print('ESTABLISHING CONNECTION TO CASSANDRA')
    # cluster = Cluster(contact_points=[cass_ip],port=9042)
    # session = cluster.connect()


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

            if 'review' in filename.split('/')[-1].split('.')[0]:

                print(f'READING REVIEW FILE {filename}')
                review = pd.read_json(filename, lines=True).head(1000)
                #print('DROPPING DUPLICATED ROWS')
                #review = review.drop_duplicates()
                #print('NORMALIZING DATES')
                review['date'] = review['date'].astype(str)
                review.rename(columns=lower_col_names(review.columns), inplace=True)
                print('UPLOADING DATAFRAME TO CASSANDRA')
                review = ps.from_pandas(review).to_spark()
                review.write\
                .format("org.apache.spark.sql.cassandra")\
                .mode('append')\
                .options(table="review", keyspace="yelp")\
                .save()
                

                # print('CREATING KEYSPACE')
                # session.execute("""
                # CREATE KEYSPACE IF NOT EXISTS henry WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
                # """)
                # print('CREATING TABLE')
                # session.execute("""
                # CREATE TABLE IF NOT EXISTS henry.review(review_id text, user_id text, business_id text, stars float, date text, text text, useful int, funny int, cool int,PRIMARY KEY(review_id))
                # """)
                # print('UPLOADING DATAFRAME TO CASSANDRA')
                # casspark.spark_pandas_insert(review,'henry','review',session,debug=True)
                print('DONE')

            elif 'tip' in filename.split('/')[-1].split('.')[0]:
                
                #### READS FILE AND MAKES TRANSFORMATION
                print(f'READING REVIEW FILE {filename}')
                tip = pd.read_json(filename,lines=True).head(1000)

                #### TRANSFORMATIONS
                print('DROPPING DUPLICATED ROWS')
                tip = tip.drop_duplicates()
                print('CLEANING STRINGS')
                tip['text'] = tip['text'].apply(lambda x: drop_bad_str(x))

                print('NORMALIZING DATES')
                tip['date'] = tip['date'].apply(lambda x: transform_dates(x)).dt.strftime('%Y-%m-%d')

                tip.rename(columns=lower_col_names(tip.columns), inplace=True)

                print('UPLOADING TO CASSANDRA')
                tip = ps.from_pandas(tip).to_spark()
                tip.write\
                .format("org.apache.spark.sql.cassandra")\
                .mode('append')\
                .options(table="tip", keyspace="yelp")\
                .save()

                print('DONE')

        print('All JSON files imported and cleaned successfully')
    else:
        print('No JSON files found')
    
def MakeQuery():
    check = spark.sql('SELECT count(*) FROM henry.yelp.review').pandas_api().to_pandas()
    check2 = spark.sql('SELECT count(*) FROM henry.yelp.tip').pandas_api().to_pandas()
    print(check)
    print(check2)


# DAG
with DAG(
    dag_id='S3_Sensor',
    start_date=datetime(2022, 12, 6),
    schedule_interval='@daily',
    default_args=default_args
) as dag:

    CheckS3 = S3KeySensor(

        #https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/s3/index.html
        task_id='S3BucketSensorNewFiles', #Name of task
        bucket_name='henrybucket999', #Support relative or full path
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
            'bucket_name': 'henrybucket999',
            'path': dest_file_path,
            }
    )

    FinishDownload = EmptyOperator(
        task_id='FinishDownload'
    )

    #Check for new files and load them
    UploadReviews = PythonOperator(
        task_id="LoadReviews",
        python_callable=LoadNewReviewsOrTips
        )

    #Make a Query to check if everything is fine
    CheckNewPricesQuery = PythonOperator(
        task_id="CheckWithQuery",
        python_callable=MakeQuery,
        )

    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


CheckS3 >> DownloadFileFromS3 >> FinishDownload
FinishDownload >> UploadReviews >> CheckNewPricesQuery >> FinishPipeline