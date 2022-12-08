import os
os.environ["JAVA_HOME"] = "/opt/java"
os.environ["SPARK_HOME"] = "/opt/spark"
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark.sparkContext.addPyFile(r'/opt/airflow/dags/transform_funcs.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/dag_initial_loading_ASTRA.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/casspark.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/tiny_functions.py')

spark.conf.set("spark.sql.catalog.AstraHenry", "com.datastax.spark.connector.datasource.CassandraCatalog")

import transform_funcs
import casspark
import pyspark.pandas as ps
ps.set_option('compute.ops_on_diff_frames', True)
# import databricks.koalas as ks
# ks.set_option('compute.ops_on_diff_frames', True)

import pandas as pd
import datetime
import json
import sqlalchemy
#import dateutil
#import pathlib
from airflow import DAG
from airflow.operators.python import PythonOperator
from time import sleep
from functools import reduce
from tiny_functions import *

# Here we create the engine that will make the connection to MySQL database
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@{address}/{db}"
                .format(user="root",
                        address = '35.239.80.227:3306',
                        pw="Henry12.BORIS99",
                        db="yelp"))


###############################
###############################
###############################

# Now we define the functions that will extract, transform and load the data in the database

###############################

def load_top_tips(df):
    ##### UPLOADS SMALL DATASET TOP TIPS TO CASSANDRA

    #### MAKES PANDAS TRANSFORMATION TO GET TOP TIPS
    top_tips = pd.DataFrame(df['business_id'].to_pandas().value_counts())
    top_tips.rename(columns={'business_id': 'number_tips'}, inplace=True)
    top_tips['business_id'] = top_tips.index
    top_tips.reset_index(drop=True, inplace=True)

    top_tips.rename(columns=transform_funcs.lower_col_names(top_tips.columns), inplace=True)

    #### CONNECT TO MySQL
    print('UPLOADING DATAFRAME TO MySQL')
    top_tips.to_sql('tip', con=engine, if_exists='replace', index=False)
    print(pd.read_sql("SELECT COUNT(*) FROM tip;", con=engine).head())
    print('DONE')
    print('DONE FOR TOP TIPS')

###############################

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
    print('GENERATING INTERACTIONS RECIEVED COLUMN')
    user_df = user.copy()
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

    user_df.rename(columns=transform_funcs.lower_col_names(user_df.columns), inplace=True)

    #### CONNECT TO MySQL
    print('UPLOADING DATAFRAME TO MySQL')
    user_df.to_pandas().to_sql('user_metrics', con=engine, if_exists='append', index=False)
    print(pd.read_sql("SELECT COUNT(*) FROM user_metrics;", con=engine).head())
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

    #### UPLOAD A SMALL SUBSET TO MySQL TOP TIPS
    print('UPLOADING SMALL DATABASE WITH TIP COUNT BY BUSINESS')
    load_top_tips(tip)

    tip.rename(columns=transform_funcs.lower_col_names(tip.columns), inplace=True)

    #### CONNECT TO MySQL
    print('UPLOADING DATAFRAME TO MySQL')
    tip.to_sql('tip', con=engine, if_exists='replace', index=False)
    print('DONE')

###############################

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

    checkin.rename(columns=transform_funcs.lower_col_names(checkin.columns), inplace=True)

    #### CONNECT TO MySQL
    print('UPLOADING DATAFRAME TO MySQL')
    checkin.to_sql('checkin', con=engine, if_exists='replace', index=False)
    print('DONE')

###############################

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

    full_data.rename(columns=transform_funcs.lower_col_names(full_data.columns), inplace=True)

    #### CONNECT TO MySQL
    print('UPLOADING DATAFRAME TO MySQL')
    full_data.to_sql('business', con=engine, if_exists='replace', index=False)
    print('DONE')

###############################

def load_review():
    print('READING REVIEW FILE')
    review = ps.read_json(r'/opt/data/initial_load/review.json')
    print('DROPPING DUPLICATED ROWS')
    review = review.drop_duplicates()
    print('NORMALIZING DATES')
    review['date'] = review['date'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')

    review.rename(columns=transform_funcs.lower_col_names(review.columns), inplace=True)

    #### CONNECT TO MySQL
    print('UPLOADING DATAFRAME TO MySQL')
    review.to_sql('review', con=engine, if_exists='replace', index=False)
    print('DONE')

###############################

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

    user.rename(columns=transform_funcs.lower_col_names(user.columns), inplace=True)

    #### CONNECT TO MySQL
    print('UPLOADING DATAFRAME TO MySQL')
    user.to_sql('user', con=engine, if_exists='replace', index=False)
    print('DONE')

###############################

def load_sentiment_business():
    sentiment = pd.read_csv(r'./data/sentiment_ok_unique.csv')

    sentiment.rename(columns=transform_funcs.lower_col_names(sentiment.columns), inplace=True)

    #### CONNECT TO MySQL
    print('UPLOADING DATAFRAME TO MySQL')
    sentiment.to_sql('user', con=engine, if_exists='replace', index=False)
    print('DONE')

###############################

# Below is the Airflow DAG that orchestrates the automated data ETL

with DAG(dag_id='LoadingMySQL',start_date=datetime.datetime(2022,8,25),schedule_interval='@once') as dag:

    t_load_tips = PythonOperator(task_id='load_tips',python_callable=load_tips)

    t_load_checkin = PythonOperator(task_id='load_checkin',python_callable=load_checkin)

    t_load_bussiness = PythonOperator(task_id='load_bussiness',python_callable=load_business)

    t_load_review = PythonOperator(task_id='load_review',python_callable=load_review)

    t_load_user = PythonOperator(task_id='load_user',python_callable=load_user)

    t_load_user_metrics = PythonOperator(task_id='load_user_metrics',python_callable=load_user_metrics)

    t_load_sentiment_business = PythonOperator(task_id='load_sentiment_business',python_callable=load_sentiment_business)

    t_load_user_metrics >> t_load_user >> t_load_checkin >> t_load_bussiness >> t_load_tips  >> t_load_review >> t_load_sentiment_business