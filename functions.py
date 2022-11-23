####################################
######## LIBRARIES IMPORT ##########
####################################
from datetime import datetime
from pathlib import Path
import os
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
import databricks.koalas as ks
ks.set_option('compute.ops_on_diff_frames', True)

####################################
#### CUSTOM FUNCTIONS IMPORT #######
####################################
from transform_funcs import *


####################################
    ######## REVIEWS  ##########
####################################

def ReviewEDA():
    df = import_json(file = 'reviews.json')
    
    df = drop_duplicates(df)

    df['user_id'] = drop_bad_ids(df, 'user_id')
    df['business_id'] = drop_bad_ids(df, 'business_id')
    df['review_id'] = drop_bad_ids(df, 'review_id')

    impute_num(df, ['useful', 'funny', 'cool'], True)

    df['date'] = transform_dates(df, 'date', '%Y-%m-%d')

    try:
        upload_to_cassandra(df, 'reviews')
        print('Reviews uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading REVIEWS to Cassandra')

    

####################################
    ######## USERS  ##########
####################################

def UserEDA():
    df = import_json(file = 'users.json')
    df = drop_duplicates(df)

    df['friends'] = df['friends'].apply(check_str_list)

    df['elite'] = df['elite'].apply(check_str_list)

    df['yelping_since'] = transform_dates(df, 'yelping_since', '%Y-%m-%d')

    try:
        upload_to_cassandra(df, 'users')
        print('Users uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading USERS to Cassandra')



####################################
    ######## BUSINESS  ##########
####################################

def BusinessEDA():
    df = import_json(file = 'business.json')
    df = drop_duplicates(df)
    
    ######## OPEN HOURS ##########
    df['hours'] = df['hours'].apply(row_hours_to_series)
    
    ######## CATEGORIES ##########
    df['categories'] = df['categories'].apply(check_str_list)

    ######## CITY/STATE ##########
    get_state_city(df)
    df.drop(['city', 'state'], axis=1, inplace=True)

    try:
        upload_to_cassandra(df, 'business')
        print('Business uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading BUSINESS to Cassandra')


####################################
    ######## CHECKIN  ##########
####################################

def CheckinEDA():
    df = import_json(file = 'checkin.json')

    df = drop_duplicates(df)
    
    df['date'] = df['date'].apply(get_date_as_list)

    df['total'] = df['date'].apply(get_total_checkins)

    try:
        upload_to_cassandra(df, 'checkin')
        print('Checkin uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading CHECKIN to Cassandra')


####################################
    ######## TIPS  ##########
####################################

def TipsEDA():

    df = import_json(file = 'tips.json')

    df = drop_duplicates(df)

    df = drop_bad_str(df, 'text')

    df['date'] = transform_dates(df, 'date', '%Y-%m-%d')

    try:
        upload_to_cassandra(df, 'tips')
        print('Tips uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading TIPS to Cassandra')

####################################
######### QUERY FUNCTIONS  ##########
####################################

def MakeQuery(query):
    sqlContext = SQLContext(spark)
    ds = sqlContext \
    .read \
    .format('org.apache.spark.sql.cassandra') \
    .options(table='business', keyspace='yelp') \
    .load()

    try:
        ds.show(10) 
        print('Query executed')
        return "OK"
    except:
        print('ERROR executing query')
        return "ERROR"



########## PENDIENTES ###############

####################################
######## SENTIMENT UPLOAD  #######
####################################

def SentimentUpload():
    return ""
