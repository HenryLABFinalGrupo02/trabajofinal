####################################
######## LIBRARIES IMPORT ##########
####################################
import requests
from datetime import datetime
from pathlib import Path
import os
import json
import ast
import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
import pyspark.pandas as ps
import databricks.koalas as ks

from transform_funcs import *

####################################
######## SPARK RUNNING ##########
####################################
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.3-bin-hadoop2.7"

spark = SparkSession.builder \
    .appName('SparkCassandraApp') \
    .config('spark.cassandra.connection.host', 'localhost') \
    .config('spark.cassandra.connection.port', '9042') \
    .config('spark.cassandra.output.consistency.level','ONE') \
    .master('local[2]') \
    .getOrCreate()


####################################
######## PATH SETTINGS ##########
####################################
path_1 = "./data/"


####################################
######## COMMON FUNCTIONS ##########
####################################
def ImporterJSON(file:str, path:Path = path_1, format:str = 'json'):
    '''
    This function imports files with spark and transforms them into DataFrame using the koala library

    Arguments:
    :: file: str of the file name
    :: path: 'path' path where the file is stored
    :: format: 'str' file format 

    Returns: 
    ---------
    Dataframe and print shape 
    '''
    path_final = path + file
    df = ps.read_json(path_final, lines=True)
    print(f"Shape of {file} is {df.shape}")
    return df

def UploadToCassandra(df, table_name):
    df.write.format("org.apache.spark.sql.cassandra")\
    .options(table=table_name, keyspace="yelp")\
    .mode('append')\
    .save()

def GetTime(datetime):
    return datetime.dt.hour()

def GetWordCount(str):
    ls = str.split()
    return len(ls)

def CountItemsFromList(value):
    ls = value.split(', ')
    return len(ls)

def Dicc(row):
    result = ast.literal_eval(row)
    return result

def GetAVG(dates_list:list):
    hours_sum = 0
    ls = dates_list.split(', ')
    list_len = len(ls)
    for date in ls:
        date_ok = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        hours_sum += date_ok.hour
    avg_checkins = hours_sum/list_len
    return round(avg_checkins)

def GetEarliestYear(dates_list:list):
    ls = dates_list.split(', ')
    earliest_year = 0
    for date in ls:
        date_ok = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        if earliest_year < date_ok.year:
            earliest_year = date_ok.year
    return earliest_year

def CountByYear(dates_list, year):
    ls = dates_list.split(', ')
    yearly_checkins = []
    for date in ls:
        date_ok = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        if date_ok.year == year:
            yearly_checkins.append(date_ok)
    return len(yearly_checkins)


####################################
    ######## REVIEWS  ##########
####################################

def ReviewEDA():
    df = ImporterJSON(file = 'reviews.json')
    
    df = drop_duplicates(df)

    df['user_id'] = drop_bad_ids(df, 'user_id')
    df['business_id'] = drop_bad_ids(df, 'business_id')
    df['review_id'] = drop_bad_ids(df, 'review_id')

    impute_num(df, ['useful', 'funny', 'cool'], True)

    df['datetime'] = df.date.astype(datetime)
    df['date'] = df.datetime.apply(lambda x: x.date())
    df['hour'] = df.datetime.dt.hour
    df['year'] = df.datetime.dt.year
    df['word_count'] = df.text.apply(GetWordCount)

    try:
        UploadToCassandra(df, 'reviews')
        print('Reviews uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading REVIEWS to Cassandra')

    

####################################
    ######## USERS  ##########
####################################

def UserEDA():
    df = ImporterJSON(file = 'users.json')
    df = drop_duplicates(df)
    df['friends_number'] = df['friends'].apply(CountItemsFromList)

    df['n_interactions_send'] = df['useful'] + df['funny'] + df['cool']

    df['n_interactions_received'] = df[[ 'compliment_hot',
    'compliment_more', 'compliment_profile', 'compliment_cute',
    'compliment_list', 'compliment_note', 'compliment_plain',
    'compliment_cool', 'compliment_funny', 'compliment_writer',
    'compliment_photos']].sum(axis=1)

    df['n_years_elite'] = df['elite'].apply(CountItemsFromList)
    df['n_years_elite'] = df['n_years_elite'].fillna(0)

    try:
        UploadToCassandra(df, 'users')
        print('Users uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading USERS to Cassandra')



####################################
    ######## BUSINESS  ##########
####################################

def BusinessEDA():
    df = ImporterJSON(file = 'business.json')
    df = drop_duplicates(df)

    ######## ATRIBUTES ##########
    attributes = pd.json_normalize(df['attributes'])
    attributes['business_id'] = df.index
    attributes.loc[attributes.BusinessParking == 'None']="{'garage': False, 'street': False, 'validated': False, 'lot': False, 'valet': False}"
    attributes.BusinessParking = attributes.BusinessParking.fillna("{'garage': False, 'street': False, 'validated': False, 'lot': False, 'valet': False}") # a los valore nulos le pogo False
    attributes.loc[attributes.Ambience == 'None']="{'romantic': False, 'intimate': False, 'classy': False, 'hipster': False, 'touristy': False, 'trendy': False, 'upscale': False, 'casual': False}"
    attributes.Ambience = attributes.Ambience.fillna("{'romantic': False, 'intimate': False, 'classy': False, 'hipster': False, 'touristy': False, 'trendy': False, 'upscale': False, 'casual': False}")
    parking = pd.json_normalize(attributes.BusinessParking.apply(Dicc))
    ambience = pd.json_normalize(attributes.Ambience.apply(Dicc))
    parking.set_index(df.index, inplace=True)
    ambience.set_index(df.index, inplace=True)
    attributes = attributes.drop(['BusinessParking', 'Ambience'], axis=1)
    attributes = pd.concat([attributes, parking, ambience], axis=1)
    
    ######## OPEN HOURS ##########
    openhours = pd.json_normalize(df['hours'])
    openhours['business_id'] = df.index
    
    ######## CATEGORIES ##########
    categories = pd.json_normalize(df['categories'])
    categories = df['categories'].str.split(', ', expand=True)
    categories = categories.T.stack().groupby('business_id').apply(list).reset_index(name='categories')
    mlb = MultiLabelBinarizer()
    cat_full = categories.join(pd.DataFrame(mlb.fit_transform(categories.pop('categories')),
                            columns=mlb.classes_,
                            index=categories.index))

    ######## COPYING TO DATALAKE ##########
    attributes.to_csv('./data/attributes.csv', index=False)
    openhours.to_csv('./data/openhours.csv', index=False)
    cat_full.to_csv('./data/categories.csv', index=False)

    df.drop(['attributes', 'hours', 'categories'], axis=1, inplace=True)

    try:
        UploadToCassandra(df, 'business')
        print('Business uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading BUSINESS to Cassandra')




####################################
    ######## CHECKIN  ##########
####################################

def CheckinEDA():
    df = ImporterJSON(file = 'checkin.json')

    df = drop_duplicates(df)
    
    df['number_visits'] = df['date'].apply(CountItemsFromList)

    df['avg_hour'] = df['date'].apply(GetAVG)

    df['earliest_year'] = df['date'].apply(GetEarliestYear)

    for x in range(2010, 2022):
        df[str(x)] = df.date.apply(CountByYear, args=(x,))

    try:
        UploadToCassandra(df, 'checkin')
        print('Checkin uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading CHECKIN to Cassandra')


####################################
    ######## TIPS  ##########
####################################

def TipsEDA():
    df = ImporterJSON(file = 'tips.json')
    df = drop_duplicates(df)

    df = drop_bad_str(df, 'text')

    df['date'] = ps.to_datetime(df['date'])
    df['year'] = df.datetime.dt.year
    df['word_count'] = df.text.apply(GetWordCount)

    try:
        UploadToCassandra(df, 'tips')
        print('Tips uploaded to Cassandra')
        return "Done"
    except:
        print('ERROR uploading TIPS to Cassandra')


####################################
######## SENTIMENT UPLOAD  #######
####################################

def SentimentUpload():
    return ""

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
