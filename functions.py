####################################
######## LIBRARIES IMPORT ##########
####################################

import requests
import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer
from datetime import datetime
import databricks.koalas as ks
from pathlib import Path
import os
import json
import ast
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.3-bin-hadoop2.7"
import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.pandas as ps


####################################
######## SPARK RUNNING ##########
####################################
spark = SparkSession.builder.master("local[*]").getOrCreate()

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
    :: path: 'path' path where the file is stored
    :: format: 'str' file format 

    Returns: 
    ---------
    Dataframe and print shape 
    '''
    path_final = path + file
    df = spark.read.load(path, format=format)
    df = df.to_koalas()
    print(df.shape)

    return df

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

    df['datetime'] = df.date.astype(datetime)
    df['date'] = df.datetime.apply(lambda x: x.date())
    df['hour'] = df.datetime.dt.hour
    df['year'] = df.datetime.dt.year
    df['word_count'] = df.text.apply(GetWordCount)

    return df



####################################
    ######## USERS  ##########
####################################

def UserEDA():
    df = ImporterJSON(file = 'users.json')

    df['friends_number'] = df['friends'].apply(CountItemsFromList)

    df['n_interactions_send'] = df['useful'] + df['funny'] + df['cool']

    df['n_interactions_received'] = df[[ 'compliment_hot',
    'compliment_more', 'compliment_profile', 'compliment_cute',
    'compliment_list', 'compliment_note', 'compliment_plain',
    'compliment_cool', 'compliment_funny', 'compliment_writer',
    'compliment_photos']].sum(axis=1)

    df['n_years_elite'] = df['elite'].apply(CountItemsFromList)
    df['n_years_elite'] = df['n_years_elite'].fillna(0)

    return df



####################################
    ######## BUSINESS  ##########
####################################

def BusinessEDA():
    df = ImporterJSON(file = 'business.json')


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

    return df



####################################
    ######## CHECKIN  ##########
####################################

def CheckinEDA():
    df = ImporterJSON(file = 'checkin.json')
    
    df['number_visits'] = df['date'].apply(CountItemsFromList)

    df['avg_hour'] = df['date'].apply(GetAVG)

    df['earliest_year'] = df['date'].apply(GetEarliestYear)

    for x in range(2010, 2022):
        df[str(x)] = df.date.apply(CountByYear, args=(x,))

    return df



####################################
    ######## TIPS  ##########
####################################

def TipsEDA():
    df = ImporterJSON(file = 'tips.json')

    df['date'] = ps.to_datetime(df['date'])
    df['year'] = df.datetime.dt.year
    df['word_count'] = df.text.apply(GetWordCount)

    return df





























# def trim_all_columns(df):
#     """
#     > If the value is a string, strip whitespace from the ends of the string. Otherwise, return the value
    
#     Arguments: 
#     --------------------------------   
#     :param df: The dataframe you want to trim

#     Return
#     --------------------------------
#     A dataframe with all the values trimmed.
#     """
#     trim_strings = lambda x: x.strip() if isinstance(x, str) else x
#     return df.applymap(trim_strings)

# def normalize_column(df, column_name):
#     """
#     > This function takes a dataframe and a column name as input, and returns a new dataframe with the
#     column normalized

#     Arguments: 
#     --------------------------------
#     :param df: the dataframe
#     :param column_name: The name of the column you want to normalize
#     """
#     df[column_name] = df[column_name].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
#     return df[column_name]

# # Source: https://stackoverflow.com/questions/9662346/python-code-to-remove-html-tags-from-a-string
# def clean_values(series, to_replace, value = '', regex = True):
#     """
#     > This function takes a pandas series, a list of values to replace, and a value to replace them with,
#     and returns a series with the values replaced.

#         Arguments: 
#     --------------------------------
#     :param series: the series you want to clean
#     :param to_replace: The value or list of values to be replaced
#     :param value: the value to replace the to_replace values with
#     :param regex: If True, assumes the to_replace pattern is a regular expression, defaults to True
#     (optional)
#     """
#     for i in to_replace:
#         series = series.str.replace(i, value, regex=regex)
#     return series

# def get_lat_lon(address, access_key = '2e843c7ee44a8f52742a8168d0121a0a', URL = "http://api.positionstack.com/v1/forward"):
#     """
#     > It takes an address and returns the latitude and longitude of that address

#      Arguments: 
#     --------------------------------   
#     :param address: The address you want to get the latitude and longitude for
#     :param access_key: This is the access key that you get from the website, defaults to
#     2e843c7ee44a8f52742a8168d0121a0a (optional)
#     :param URL: The URL of the API endpoint, defaults to http://api.positionstack.com/v1/forward
#     (optional)

#     Return
#     --------------------------------
#     A tuple of latitude and longitude
#     """
#     PARAMS = {'access_key': access_key, 'query': address}
#     r = requests.get(url = URL, params = PARAMS)
#     data = r.json()
#     return data['data'][0]['latitude'], data['data'][0]['longitude']

# def run_exps(X_train: pd.DataFrame , y_train: pd.DataFrame, X_test: pd.DataFrame, y_test: pd.DataFrame) -> pd.DataFrame:
#     """
#     > This function takes in training and test data, and then runs a bunch of models on it, returning a
#     dataframe of the results
    
#     Arguments: 
#     --------------------------------
#     :param X_train: training split 
#     :param y_train: training target vector
#     :param X_test: test split
#     :param y_test: test target vector

#     Types: 
#     --------------------------------
#     :type X_train: pd.DataFrame
#     :type y_train: pd.DataFrame
#     :type X_test: pd.DataFrame
#     :type y_test: pd.DataFrame

#     Return
#     --------------------------------
#     A dataframe of predictions
#     """
    
#     dfs = []

#     dt = DecisionTreeClassifier(max_depth=1)

#     models = [
#         ('LogReg', LogisticRegression()), 
#         ('RF', RandomForestClassifier()),
#         ('KNN', KNeighborsClassifier()),
#         ('GNB', GaussianNB()),
#         ('XGB', XGBClassifier()),
#         ('ADA', AdaBoostClassifier(base_estimator=dt))
#         ]
#     results = []
#     names = []

#     scoring = ['accuracy', 'precision_weighted', 'recall_weighted', 'f1_weighted', 'roc_auc']

#     target_names = ['malignant', 'benign']

#     for name, model in models:
#             kfold = model_selection.KFold(n_splits=5, shuffle=True, random_state=90210)
#             cv_results = model_selection.cross_validate(model, X_train, y_train, cv=kfold, scoring=scoring)
#             clf = model.fit(X_train, y_train)
#             y_pred = clf.predict(X_test)
#             print(name)
#             print(classification_report(y_test, y_pred, target_names=target_names))
            
#     results.append(cv_results)
#     names.append(name)
#     this_df = pd.DataFrame(cv_results)
#     this_df['model'] = name
#     dfs.append(this_df)
#     final = pd.concat(dfs, ignore_index=True)
#     return final
