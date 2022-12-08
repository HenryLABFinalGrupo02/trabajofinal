### Importing libraries, some libraries imported within the function for Airflow DAG optimization
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.taskinstance import TaskInstance as ti
from tempfile import NamedTemporaryFile
import os
import sqlalchemy

#Check version of Airflow and Pip Amazon installed 
#Sensor to check if the file is loaded in the bucket
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

#Hook to connect to S3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#Set path for new files downloaded from Bucket 
dest_file_path = '/opt/data/minio/'

# Default arguments
default_args = {
    'owner': 'DS04_TF_G2_Henry',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

def lower_col_names(cols):
    '''
    Function to lower case all column names
    
    Parameters
    ----------
    cols: list
        List of column names
    
    Returns
    -------
    new_names: dict
        Dictionary with the new column names
    '''
    new_names = {}
    for x in cols:
        new_names[x] = x.lower()
    return new_names    

def DownloadAndRenameFile(bucket_name:str, path:str):
    '''
    Function to download the last file in the bucket and rename it to the original name

    Parameters
    ----------
    bucket_name: str
        Name of the bucket where the file is located
    path: str
        Path where the file will be downloaded

    Returns
    -------
    file_name: str
        Path where the file was downloaded
    '''
    hook = S3Hook('minio_conn') ## Hook to connect to S3, see Airflow Connections
    files = hook.list_keys(bucket_name=bucket_name) ## 
    key = files[-1]
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=path)
    RenameFile(file_name, key)
    return file_name

def RenameFile(file_name:str, new_name:str) -> None:
    '''
    Function to rename a file

    Parameters
    ----------
    file_name: str
        Path where the file is located
    new_name: str
        New name of the file

    Returns
    -------
    None
    '''

    downloaded_file_path = '/'.join(file_name.split('/')[:-1])
    os.rename(src=file_name, dst=f'{downloaded_file_path}/{new_name}')
    print('Renamed successfully')

def LoadNewReviewsOrTips():
    '''
    Function to load new reviews or tips to the database
    
    '''

    ### Importing libraries and custom functions for ETL
    from transform_funcs import transform_dates, drop_bad_str, lower_col_names
    import pandas as pd
    import glob
    
    print('ESTABLISHING CONNECTION TO MYSQL')
    engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@{address}/{db}"
            .format(user="root",
                    address = '35.239.80.227:3306',
                    pw="Henry12.BORIS99",
                    db="yelp"))

    ### Get all files in the folder
    path = dest_file_path
    try:
        all_json = glob.glob(path + "/*.json")

        if len(all_json) == 0:
            raise FileNotFoundError('No files found in the folder')
    except:
        print('Error with path or files GLOB ERROR')

    #Get all JSON in the folder
    # If there are files, read them and upload to MySQL
    if len(all_json) > 0:
        for filename in all_json:

            # If the file is a review, read it and upload to MySQL
            if 'review' in filename.split('/')[-1].split('.')[0]:

                print(f'READING REVIEW FILE {filename}')
                review = pd.read_json(filename)

                print('NORMALIZING DATES')
                review['date'] = review['date'].astype(str)

                print('CLEANING COLUMNS')
                review.rename(columns=lower_col_names(review.columns), inplace=True)

                print('UPLOADING DATAFRAME TO MYSQL')
                review.to_sql('review', con=engine, if_exists='append', index=False)

            # If the file is a tip, read it and upload to MySQL
            elif 'tip' in filename.split('/')[-1].split('.')[0]:
                
                #### READS FILE AND MAKES TRANSFORMATION
                print(f'READING REVIEW FILE {filename}')
                tip = pd.read_json(filename,lines=True)

                #### TRANSFORMATIONS
                print('DROPPING DUPLICATED ROWS')
                tip = tip.drop_duplicates()

                #### TRANSFORMATIONS
                print('CLEANING STRINGS')
                tip['text'] = tip['text'].apply(lambda x: drop_bad_str(x))

                print('NORMALIZING DATES')
                tip['date'] = tip['date'].apply(lambda x: transform_dates(x)).dt.strftime('%Y-%m-%d')
                tip.rename(columns=lower_col_names(tip.columns), inplace=True)

                print('CONNECTING TO DATABASE and UPLOADING')
                engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@{address}/{db}"
                            .format(user="root",
                                    address = '35.239.80.227:3306',
                                    pw="Henry12.BORIS99",
                                    db="yelp"))

                tip.to_sql('tip', con=engine, if_exists='replace', index=False)

                print(pd.read_sql("SELECT COUNT(*) FROM tip;", con=engine).head())

        print('All JSON files imported and cleaned successfully')
    else:
        print('No JSON files found')
    
def MakeQuery():
    '''
    Function to make a query to the database
    '''
    import pandas as pd
    engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@{address}/{db}"
            .format(user="root",
                    address = '35.239.80.227:3306',
                    pw="Henry12.BORIS99",
                    db="yelp"))

    check = pd.read_sql_query('SELECT count(*) FROM review', con=engine)
    check2 = pd.read_sql_query('SELECT count(*) FROM tip', con=engine)
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
        poke_interval=5, #How often to check for new files
        timeout=120 #How long to wait for new files
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

    #Finish the pipeline
    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


CheckS3 >> DownloadFileFromS3 >> FinishDownload
FinishDownload >> UploadReviews >> CheckNewPricesQuery >> FinishPipeline





'''
CODE TO IMPLEMENT FOR CASSANDRA
# import casspark
# from cassandra.cluster import Cluster
# cass_ip = 'cassandra'
# import pyspark as ps
# print('ESTABLISHING CONNECTION TO CASSANDRA')
# cluster = Cluster(contact_points=[cass_ip],port=9042)
# session = cluster.connect()

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
# print('DONE')
'''

