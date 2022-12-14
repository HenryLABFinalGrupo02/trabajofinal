import os
os.environ["JAVA_HOME"] = "/opt/java"
os.environ["SPARK_HOME"] = "/opt/spark"
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark.sparkContext.addPyFile(r'/opt/airflow/dags/transform_funcs.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/dag_initial_loading.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/casspark.py')
import pyspark.pandas as ps
import databricks.koalas as ks
ks.set_option('compute.ops_on_diff_frames', True)
ps.set_option('compute.ops_on_diff_frames', True)
import pandas as pd
import transform_funcs
import pathlib
import datetime
from airflow import DAG
import dateutil
from airflow.operators.python import PythonOperator
import casspark
from cassandra.cluster import Cluster


cass_ip = 'cassandra'

###############################
###############################
###############################

# Below we define the functions that will extract, transform and load the data in the database

###############################

def load_tips():
    print('READING TIPS FILE')
    tip = ps.read_json(r'/opt/data/initial_load/tip.json').head(10000)
    print('DROPPING DUPLICATED ROWS')
    tip = tip.drop_duplicates()
    print('CLEANING STRINGS')
    tip = tip[tip['text'].apply(transform_funcs.drop_bad_str) != 'REMOVE_THIS_ROW']
    print('NORMALIZING DATES')
    tip['date'] = tip['date'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')
    print('ESTABLISHING CONNECTION TO CASSANDRA')
    cluster = Cluster(contact_points=[cass_ip],port=9042)
    session = cluster.connect()
    print('CREATING KEYSPACE')
    session.execute("""
CREATE KEYSPACE IF NOT EXISTS henry WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
""")
    print('CREATING TABLE')
    session.execute("""
CREATE TABLE IF NOT EXISTS henry.tip(business_id text, compliment_count int, date text, text text, user_id text,PRIMARY KEY((business_id,date,user_id)))
""")
    print('UPLOADING DATAFRAME TO CASSANDRA')
    casspark.spark_pandas_insert(tip,'henry','tip',session,debug=True)
    print('DONE')

###############################

def load_checkin():
    print('READING TIPS FILE')
    checkin = ps.read_json(r'/opt/data/initial_load/checkin.json').head(10000)
    print('DROPPING DUPLICATED ROWS')
    checkin = checkin.drop_duplicates()
    print("CALCULATING TOTAL CHECKINS")
    checkin['total'] = checkin['date'].apply(lambda x: get_len(x))
    print("GETTING AVERAGE HOUR")
    checkin['avg_hour'] = checkin.date.apply(transform_funcs.get_avg_checkins)
    print('NORMALIZING DATES')
    checkin['date'] = checkin['date'].apply(transform_funcs.get_date_as_list)
    print('ESTABLISHING CONNECTION TO CASSANDRA')
    cluster = Cluster(contact_points=[cass_ip],port=9042)
    session = cluster.connect()
    print('CREATING KEYSPACE')
    session.execute("""
CREATE KEYSPACE IF NOT EXISTS henry WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
""")
    print('CREATING TABLE')
    session.execute("""
CREATE TABLE IF NOT EXISTS henry.checkin(business_id text, date list<text>,PRIMARY KEY(business_id))
""")
    print('UPLOADING DATAFRAME TO CASSANDRA')
    casspark.spark_pandas_insert(checkin,'henry','checkin',session,debug=True)
    print('DONE')

###############################

def load_bussiness():
    print('READING BUSINESS FILE')
    business = ps.read_json(r'/opt/data/initial_load/business.json', lines=True).head(10000)
    print('DROPPING DUPLICATED ROWS')
    business = business.drop_duplicates()
    print('JOINING CITY AND STATE')
    business['state_city']  = transform_funcs.get_state_city(business['city'],business['state'])
    business = business[[column for column  in list(business.columns) if column not in ['city','state']]]
    print('NORMALIZING HOURS') #df['hours'] = df['hours'].apply(row_hours_to_series)
    business['hours'] = transform_funcs.row_hours_to_series(business['hours'])
    print('NORMALIZING ATTRIBUTES')
    business['attributes'] = transform_funcs.row_att_to_series(business['attributes'])
    print('NORMALIZING CATEGORIES')
    business['categories'] = business['categories'].apply(transform_funcs.check_str_list)
    print('ESTABLISHING CONNECTION TO CASSANDRA')
    cluster = Cluster(contact_points=[cass_ip],port=9042)
    session = cluster.connect()
    print('CREATING KEYSPACE')
    session.execute("""
CREATE KEYSPACE IF NOT EXISTS henry WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
""")
    print('CREATING TABLE')
    session.execute("""
CREATE TABLE IF NOT EXISTS henry.business(address text, attributes list<frozen <list<text>>>, business_id text, categories list<text>, hours list<frozen <list<int>>>, is_open int, latitude float, longitude float, name text, postal_code text, review_count int, stars float, state_city list<text>,PRIMARY KEY(business_id))
""")
    print('UPLOADING DATAFRAME TO CASSANDRA')
    casspark.spark_pandas_insert(business,'henry','business',session,debug=True)
    print('DONE')

###############################

def load_review():
    print('READING REVIEW FILE')
    review = ps.read_json(r'/opt/data/initial_load/review.json', lines=True).head(10000)
    print('DROPPING DUPLICATED ROWS')
    review = review.drop_duplicates()
    print('NORMALIZING DATES')
    review['date'] = review['date'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')
    print('ESTABLISHING CONNECTION TO CASSANDRA')
    cluster = Cluster(contact_points=[cass_ip],port=9042)
    session = cluster.connect()
    print('CREATING KEYSPACE')
    session.execute("""
CREATE KEYSPACE IF NOT EXISTS henry WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
""")
    print('CREATING TABLE')
    session.execute("""
CREATE TABLE IF NOT EXISTS henry.review(review_id text, user_id text, business_id text,
                                        stars float, date string, text string,
                                        useful int, funny int, cool int, PRIMARY KEY(review_id))
""")
    print('UPLOADING DATAFRAME TO CASSANDRA')
    casspark.spark_pandas_insert(review,'henry','review',session,debug=True)
    print('DONE')

###############################

def load_user():
    print('READING USER FILE')
    user = ps.read_json(r'/opt/data/initial_load/user.json', lines=True).head(10000)
    print('DROPPING DUPLICATED ROWS')
    user = user.drop_duplicates()
    print('NORMALIZING DATES')
    user['yelping_since'] = user['yelping_since'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')
    #print('TRANSFORMING ELITE')
    #user['elite'] = transform_funcs.get_elite_list(user['elite'])

    print('DROPPING ELITE & FRIENDS') #elite list <int>, friends varchar,
    user = user.drop(['elite', 'friends'], axis=1)
    print('ESTABLISHING CONNECTION TO CASSANDRA')
    cluster = Cluster(contact_points=[cass_ip],port=9042)
    session = cluster.connect()
    print('CREATING KEYSPACE')
    session.execute("""
CREATE KEYSPACE IF NOT EXISTS henry WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
""")
    # print('DROPPING TABLE')
    # session.execute('DROP TABLE [IF EXISTS] henry.user')

    print('CREATING TABLE')
    session.execute("""
CREATE TABLE IF NOT EXISTS henry.user(user_id text, name text, review_count int, 
                                      yelping_since text, 
                                      useful int, funny int, cool int, fans int,
                                      average_stars float,
                                      compliment_hot float, compliment_more int, 
                                      compliment_profile int, compliment_cute int, 
                                      compliment_list int, compliment_note int, 
                                      compliment_plain int, compliment_cool int, 
                                      compliment_funny int, compliment_writer int,
                                      compliment_photos int, PRIMARY KEY(user_id))
""")
    print('UPLOADING DATAFRAME TO CASSANDRA')
    casspark.spark_pandas_insert(user,'henry','user',session,debug=True)
    print('DONE')


###############################

# Below is the Airflow DAG that orchestrates the automated data ETL

with DAG(dag_id='Initial_Load',start_date=datetime.datetime(2022,8,25),schedule_interval='@once') as dag:

    t_load_tips = PythonOperator(task_id='load_tips',python_callable=load_tips)

    t_load_checkin = PythonOperator(task_id='load_checkin',python_callable=load_checkin)

    t_load_bussiness = PythonOperator(task_id='load_bussiness',python_callable=load_bussiness)

    t_load_review = PythonOperator(task_id='load_review',python_callable=load_review)

    t_load_user = PythonOperator(task_id='load_user',python_callable=load_user)

    t_load_user >> t_load_review >> t_load_checkin >> t_load_tips  >> t_load_bussiness