####################################
######## LIBRARIES IMPORT ##########
####################################
from datetime import datetime
import os
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
import databricks.koalas as ks
import pyspark.pandas as ps

####################################
#### CUSTOM FUNCTIONS IMPORT #######
####################################

print('IMPORTING EXTRA FUNCTIONS')

from transform_funcs import *


####################################
    ######## BUSINESS  ##########
####################################

print("BUSINESS")

def BusinessEDA():
    print('IMPORTING BUSINESS')
    df = import_json(file = 'business.json', path = '/opt/data/')
    print('DROPPING DUPLICATES')
    df = drop_duplicates(df)
    
    ######## OPEN HOURS ##########
    #df['hours'] = df['hours'].apply(row_hours_to_series)
    
    #print('CHECKING STRINGS')
    ######## CATEGORIES ##########
    #df['categories'] = df['categories'].apply(check_str_list)

    ######## CITY/STATE ##########
    #print('GETTING STATE_CITY COL')
    #get_state_city(df)
    print('DROPPING CITY & STATE COLS')
    df = df.drop(['city', 'state'], axis=1)

    print('TRYING TO UPLOAD')

    try:
        upload_to_cassandra(df, 'business')
        print('Business uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading BUSINESS to Cassandra')


####################################
    ######## CHECKIN  ##########
####################################

print('CHECKIN')

def CheckinEDA():
    print('IMPORTING')
    df = import_json(file = 'checkin.json', path = '/opt/data/')

    print('DROPPING DUPS')
    df = drop_duplicates(df)
    
    #print('GETTING DATE LIST')
    #df['date'] = df['date'].apply(get_date_as_list)

    #print('GETTING TOTAL')
    #df['total'] = df['date'].apply(get_total_checkins)
    print('TRYING TO UPLOAD')
    try:
        upload_to_cassandra(df, 'checkin')
        print('Checkin uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading CHECKIN to Cassandra')


####################################
    ######## TIPS  ##########
####################################

print('TIPS')

def TipsEDA():
    print('IMPORTING')
    df = import_json(file = 'tip.json', path = '/opt/data/')

    print('DROPPING DUPS')
    df = drop_duplicates(df)

    #print('DROPPING BAD STRS')
    #df = drop_bad_str(df, 'text')

    #print('TRANSFORMING DATES')
    #df['date'] = transform_dates(df, 'date', '%Y-%m-%d')

    try:
        upload_to_cassandra(df, 'tips')
        print('Tips uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading TIPS to Cassandra')


####################################
    ######## USERS  ##########
####################################

print('USERS')

def UserEDA():
    print('IMPORTING')
    df = import_json(file = 'user.json', path = '/opt/data/')
    
    print('DROPPING DUPS')
    df = drop_duplicates(df)

    #print('CHECKING STR LIST')
    #df['friends'] = df['friends'].apply(check_str_list)

    #print('CHECKING 2ND STR LIST')
    #df['elite'] = df['elite'].apply(check_str_list)

    #print('TRANSFORMING DATES')
    #df['yelping_since'] = transform_dates(df, 'yelping_since', '%Y-%m-%d')

    try:
        upload_to_cassandra(df, 'users')
        print('Users uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading USERS to Cassandra')



####################################
    ######## REVIEWS  ##########
####################################
print('REVIEW')

def ReviewEDA():
    df = import_json(file = 'review_2021.json', path = '/opt/data/')
    
    print('DELETING DUPLICATES')
    df = drop_duplicates(df)

    #print('DELETING BAD ID')
    #df['user_id'] = drop_bad_ids(df, 'user_id')
    #df['business_id'] = drop_bad_ids(df, 'business_id')
    #df['review_id'] = drop_bad_ids(df, 'review_id')

    #print('IMPUTING NEGATIVE VOTES')
    #impute_num(df, ['useful', 'funny', 'cool'], True) ##### REALLY SLOW

    #print('TRANSFORMING DATES')
    #df['date'] = transform_dates(df, 'date', '%Y-%m-%d')

    try:
        upload_to_cassandra(df, 'reviews')
        print('Reviews uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading REVIEWS to Cassandra')


####################################
######### QUERY FUNCTIONS  ##########
####################################

def MakeQuery(query):
    # sqlContext = SQLContext(spark)
    # ds = sqlContext \
    # .read \
    # .format('org.apache.spark.sql.cassandra') \
    # .options(table='business', keyspace='yelp') \
    # .load()

    # try:
    #     ds.show(10) 
    #     print('Query executed')
    #     return "OK"
    # except:
    #     print('ERROR executing query')
    #     return "ERROR"
    print('Query executed')
    return "This Works"


########## PENDIENTES ###############

####################################
######## SENTIMENT UPLOAD  #######
####################################

def SentimentUpload():
    return ""

