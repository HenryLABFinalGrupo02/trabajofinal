import os
os.environ["JAVA_HOME"] = "/opt/java"
os.environ["SPARK_HOME"] = "/opt/spark"
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark.sparkContext.addPyFile(r'/opt/airflow/dags/transform_funcs.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/dag_initial_loading_cass.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/casspark.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/tiny_functions.py')

spark.conf.set("spark.sql.catalog.AstraHenry", "com.datastax.spark.connector.datasource.CassandraCatalog")

import transform_funcs
#import casspark
import pyspark.pandas as ps
ps.set_option('compute.ops_on_diff_frames', True)
# import databricks.koalas as ks
# ks.set_option('compute.ops_on_diff_frames', True)

import pandas as pd
import datetime
#import json
#import dateutil
#import pathlib
from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
#from cassandra.auth import PlainTextAuthProvider
from time import sleep
from functools import reduce
from tiny_functions import *

cass_ip = 'cassandra'

# cloud_config= {'secure_connect_bundle': r'/opt/data/cassandra/secure-connect-henry.zip'}
# auth_provider = PlainTextAuthProvider(json.load(open(r'/opt/data/cassandra/log_in.json'))['log_user'], json.load(open(r'/opt/data/cassandra/log_in.json'))['log_password'])

def connect_to_cassandra():
    print('ESTABLISHING CONNECTION TO CASSANDRA')
    cluster = Cluster(contact_points=[cass_ip],port=9042)
    session = cluster.connect()
    return session

def lower_col_names(cols):
    new_names = {}
    for x in cols:
        new_names[x] = x.lower()
    return new_names    

def load_top_tips(df):
    ##### UPLOADS SMALL DATASET TOP TIPS TO CASSANDRA

    #### MAKES PANDAS TRANSFORMATION TO GET TOP TIPS
    top_tips = pd.DataFrame(df['business_id'].to_pandas().value_counts())
    top_tips.rename(columns={'business_id': 'number_tips'}, inplace=True)
    top_tips['business_id'] = top_tips.index
    top_tips.reset_index(drop=True, inplace=True)

    top_tips.rename(columns=lower_col_names(top_tips.columns), inplace=True)

    top_tips2 = ps.from_pandas(top_tips)

    #### CONNECT TO CASSANDRA
    print('ESTABLISHING CONNECTION TO CASSANDRA FOR TOP TIPS')
    session = connect_to_cassandra()

    session.execute("""
CREATE KEYSPACE IF NOT EXISTS yelp WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
""")

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.top_tips_full;")

    print('CREATING TABLE FOR TOP TIPS')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.top_tips_full(business_id text, number_tips int, PRIMARY KEY((business_id)))
    """)
    sleep(5)
    #### UPLOAD DATAFRAME TO CASSANDRA
    print('UPLOADING DATAFRAME TO CASSANDRA FOR TOP TIPS')
    top_tips2.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="top_tips_full", keyspace="yelp")\
    .save()
    print('DONE')
    print('DONE FOR TOP TIPS')


def load_user_metrics(): 
    """
    The function takes a dataframe of users and returns a dataframe with the influencer score for each
    user

    :param user: the user dataframe
    :return: A dataframe with the columns: n_interactions_received, n_interactions_send, fans,
    friends_number, Score_influencer, Influencer, user_id
    """
    print('READING USER FILE')
    user = ps.read_json(r'/opt/data/initial_load/user.json')
    print('DROPPING DUPLICATED ROWS')
    user = user.drop_duplicates()

    print('NORMALIZING DATES')
    user['yelping_since'] = user['yelping_since'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')
    print('COPYING USER DF')
    user_df = user.copy()
    print('GENERATING INTERACTIONS RECIEVED COLUMN')
    user_df['n_ints_rec'] = user_df[[ 'compliment_hot',
    'compliment_more', 'compliment_profile', 'compliment_cute',
    'compliment_list', 'compliment_note', 'compliment_plain',
    'compliment_cool', 'compliment_funny', 'compliment_writer',
    'compliment_photos']].sum(axis=1)
    print('GENERATING INTERACTIONS SEND COLUMN')
    user_df['n_interactions_send'] = user_df['useful'] + user_df['funny'] + user_df['cool']
    print('GENERATING FRIENDS NUM COLUMN')
    user_df['friends_number'] = user_df.friends.apply(get_len)
    print('GENERATING INFLUENCER COLUMN')
    user_df['Influencer'] = user_df['n_ints_rec'] / (1 + user_df['friends_number'] + user_df['fans'])
    user_df['Influencer'].fillna(0, inplace = True)
    print('GENERATING INFLUENCER SCORE COLUMN')
    user_df['Influencer_Score'] = 1 - (1 / (1 + user_df['Influencer']))
    print('GENERATING INFLUENCER 2 COLUMN')
    user_df['Influencer_2'] = user_df['n_ints_rec'] / (1 + user_df['fans'])
    user_df['Influencer_2'].fillna(0, inplace = True)
    print('GENERATING INFLUENCER SCORE 2 COLUMN')
    user_df['Influencer_Score_2'] = 1 - (1 / (1 + user_df['Influencer_2']))

    print('GENERATING NEW DF')
    user_df = user_df[['user_id', 'n_ints_rec', 'n_interactions_send', 'fans', 'friends_number',
    'Influencer', 'Influencer_Score', 'Influencer_2', 'Influencer_Score_2']]
    print('TRANSFORMATIONS FOR USER METRICS DONE')

    user_df.rename(columns=lower_col_names(user_df.columns), inplace=True)

    print('STABLISHING CONNECTION TO ASTRA')
    session = connect_to_cassandra()
    
    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.user_metrics_full;")
    
    print('CREATING TABLE FOR USER METRICS')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.user_metrics_full(
        user_id text, 
        n_ints_rec int,
        n_interactions_send int,
        fans int,
        friends_number int,
        Influencer float,
        Influencer_Score float,
        Influencer_2 float,
        Influencer_Score_2 float,
        PRIMARY KEY(user_id));
""") #friends list <text>,
    
    sleep(5)

    print('UPLOADING DATAFRAME TO CASSANDRA FOR USER METRICS')
    user_df.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="user_metrics_full", keyspace="yelp")\
    .save()
    print('DONE')



###############################

def load_tips():
    #### READS FILE AND MAKES TRANSFORMATION
    print('READING TIPS FILE')
    tip = ps.read_json(r'/opt/data/initial_load/tip.json')

    #### TRANSFORMATIONS
    print('DROPPING DUPLICATED ROWS')
    tip = tip.drop_duplicates()
    print('CLEANING STRINGS')
    tip = tip[tip['text'].apply(transform_funcs.drop_bad_str) != 'REMOVE_THIS_ROW']
    print('NORMALIZING DATES')
    tip['date'] = tip['date'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')

    #### UPLOAD A SMALL SUBSET TO CASSANDRA TOP TIPS
    print('UPLOADING SMALL DATABASE WITH TIP COUNT BY BUSINESS')
    load_top_tips(tip)

    tip.rename(columns=lower_col_names(tip.columns), inplace=True)

    #### CONNECT TO CASSANDRA


    session = connect_to_cassandra()

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.tip_full;")

    #### CREATE KEYSPACE AND TABLE
    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.tip_full(business_id text, date text, user_id text, compliment_count int, text text,PRIMARY KEY((business_id,date,user_id)))
    """)

    sleep(5)
    #### UPLOAD DATAFRAME TO CASSANDRA
    print('UPLOADING DATAFRAME TO CASSANDRA')
    tip.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="tip_full", keyspace="yelp")\
    .save()
    print('DONE')


def load_checkin():
    #### READS FILE AND MAKES TRANSFORMATION
    print('READING TIPS FILE')
    checkin = ps.read_json(r'/opt/data/initial_load/checkin.json')

    #### TRANSFORMATIONS
    print('DROPPING DUPLICATED ROWS')
    checkin = checkin.drop_duplicates()

    print("CALCULATING TOTAL CHECKINS")
    checkin['total'] = checkin['date'].apply(lambda x: get_len(x))

    print("GETTING AVERAGE HOUR")
    checkin['avg_hour'] = checkin.date.apply(transform_funcs.get_avg_checkins)

    print('NORMALIZING DATES')
    checkin['date'] = checkin['date'].apply(transform_funcs.get_date_as_list)

    checkin.rename(columns=lower_col_names(checkin.columns), inplace=True)

    session = connect_to_cassandra()

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.checkin_full;")

    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.checkin_full(business_id text, date list<text>, total int,PRIMARY KEY(business_id))
    """)

    sleep(5)
    #### UPLOAD DATAFRAME TO CASSANDRA
    print('UPLOADING DATAFRAME TO CASSANDRA')
    checkin.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="checkin_full", keyspace="yelp")\
    .save()
    print('DONE')


def load_business():
    print('READING BUSINESS FILE')
    business = pd.read_json(r'/opt/data/initial_load/business.json', lines=True)
    

    print('TRANSFORMING ATTRIBUTES')
    atributtes = etl_atributtes(business)

    print('TRANSFORMING HOURS')
    hours = etl_hours(business)

    print('TRANSFORMING CATEGORIES') #FILLLED NA
    categories = etl_categories(business)

    print('GPS CLUSTERING')
    gps = etl_gps(business)

    print('FIXING RestaurantsPriceRange2 AND DELIVERY')
    atributtes['RestaurantsPriceRange2'] = pd.to_numeric(atributtes['RestaurantsPriceRange2'])
    
    print(atributtes['delivery'].unique())

    print('MERGING DATAFRAMES')
    data_frames = [business, atributtes, categories, hours, gps]
    full_data = reduce(lambda left,right: pd.merge(left,right,on='business_id', how='left'), data_frames)
    full_data = full_data.drop(['attributes', 'hours', 'city', 'state', 'categories', 'latitude_y', 'longitude_y'], axis=1)

    full_data.rename(columns={'Home Services':'HomeServices','Beauty & Spas':'BeautyAndSpas', 'Health & Medical':'HealthAndMedical','Local Services':'LocalServices', '7days':'SevenDays'}, inplace=True)

    full_data['mean_open_hour'] = full_data.mean_open_hour.astype(str)
    full_data['mean_close_hour'] = full_data.mean_close_hour.astype(str)
    full_data['RestaurantsPriceRange2'] = full_data.RestaurantsPriceRange2.astype(str)

    full_data.rename(columns=lower_col_names(full_data.columns), inplace=True)

    print('CONVERTING TO PYSPAK PANDAS')
    full_data2 = ps.from_pandas(full_data)

    session = connect_to_cassandra()

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.business_full;")

    print(f'FULL DATA COLUMNS:\n{full_data.columns.to_list()}')

    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.business_full(
        business_id text,
        name text,
        address text,
        postal_code text,
        latitude_x float,
        longitude_x float,
        stars float,
        review_count int,
        is_open int,
        good_ambience int,
        garage int,
        BusinessAcceptsCreditCards int,
        RestaurantsPriceRange2 text,
        BikeParking int,
        WiFi int,
        delivery int,
        GoodForKids int,
        OutdoorSeating int,
        RestaurantsReservations int,
        HasTV int,
        RestaurantsGoodForGroups int,
        Alcohol int,
        ByAppointmentOnly int,
        Caters int,
        RestaurantsAttire int,
        NoiseLevel int,
        WheelchairAccessible int,
        RestaurantsTableService int,
        meal_diversity int,
        Restaurants int,
        Food int,
        Shopping int,
        HomeServices int,
        BeautyAndSpas int,
        Nightlife int,
        HealthAndMedical int,
        LocalServices int,
        Bars int,
        Automotive int,
        total_categories int,
        SevenDays int,
        weekends int,
        n_open_days int,
        mean_total_hours_open float,
        mean_open_hour text,
        mean_close_hour text,
        areas int,
        PRIMARY KEY(business_id))
    """)

    sleep(5)
    print('UPLOADING DATAFRAME TO CASSANDRA')
    full_data2.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="business_full", keyspace="yelp")\
    .save()
    print('DONE')


def load_review():
    print('READING REVIEW FILE')
    review = ps.read_json(r'/opt/data/initial_load/review.json')
    print('DROPPING DUPLICATED ROWS')
    review = review.drop_duplicates()
    print('NORMALIZING DATES')
    review['date'] = review['date'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')

    review.rename(columns=lower_col_names(review.columns), inplace=True)

    session = connect_to_cassandra()

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.review_full;")

    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.review_full(
        review_id text,
        user_id text,
        business_id text,
        stars float,
        date text,
        text text,
        useful int,
        funny int,
        cool int,
        PRIMARY KEY(review_id))
    """)

    sleep(5)
    print('UPLOADING DATAFRAME TO CASSANDRA')
    review.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="review_full", keyspace="yelp")\
    .save()
    print('DONE')



def load_user():
    print('READING USER FILE')
    user = ps.read_json(r'/opt/data/initial_load/user.json')
    print('DROPPING DUPLICATED ROWS')
    user = user.drop_duplicates()

    print('NORMALIZING DATES')
    user['yelping_since'] = user['yelping_since'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')

    # print('CREATING NEW FEATURES AND UPLOADING THEM')
    # influencer_Score_2(user)

    print('DROPPING ELITE & FRIENDS')
    user = user.drop(['friends', 'elite'], axis=1)

    print('USER COLS')
    print(user.columns)

    user.rename(columns=lower_col_names(user.columns), inplace=True)

    session = connect_to_cassandra()

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.user_full;")

    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.user_full(
        user_id text,
        name text,
        review_count int,
        yelping_since text,
        useful int,
        funny int,
        cool int,
        fans int,
        average_stars float,
        compliment_hot float,
        compliment_more int,
        compliment_profile int,
        compliment_cute int,
        compliment_list int,
        compliment_note int,
        compliment_plain int ,
        compliment_cool int,
        compliment_funny int,
        compliment_writer int,
        compliment_photos int,
    PRIMARY KEY(user_id))
""")

    sleep(5)
    print('UPLOADING DATAFRAME TO CASSANDRA')
    user.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="user_full", keyspace="yelp")\
    .save()
    print('DONE')



def load_sentiment_business():
    sentiment = pd.read_csv(r'./data/initial_load/sentiment_ok_unique.csv')

    sentiment.rename(columns=lower_col_names(sentiment.columns), inplace=True)

    sentiment2 = ps.from_pandas(sentiment)

    session = connect_to_cassandra()
    
    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.sentiment_business_full;")

    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.sentiment_business_full(
        business_id text,
        neg_reviews int, 
        pos_reviews int,
        PRIMARY KEY(business_id))
""")
    sleep(5)
    print('UPLOADING DATAFRAME TO CASSANDRA')
    sentiment2.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="sentiment_business_full", keyspace="yelp")\
    .save()
    print('DONE')




#DAG de Airflow
with DAG(dag_id='DAG_Initial_Load',start_date=datetime.datetime(2022,8,25),schedule_interval='@once') as dag:

    t_load_tips = PythonOperator(task_id='load_tips',python_callable=load_tips)

    t_load_checkin = PythonOperator(task_id='load_checkin',python_callable=load_checkin)

    t_load_bussiness = PythonOperator(task_id='load_bussiness',python_callable=load_business)

    t_load_review = PythonOperator(task_id='load_review',python_callable=load_review)

    t_load_user = PythonOperator(task_id='load_user',python_callable=load_user)

    t_load_user_metrics = PythonOperator(task_id='load_user_metrics',python_callable=load_user_metrics)

    t_load_sentiment_business = PythonOperator(task_id='load_sentiment_business',python_callable=load_sentiment_business)

    t_load_user_metrics >> t_load_user >> t_load_checkin >> t_load_bussiness >> t_load_tips  >> t_load_review >> t_load_sentiment_business