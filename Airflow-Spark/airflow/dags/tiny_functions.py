import pandas as pd
import json
import ast
import numpy as np
import datetime
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.cluster import KMeans

def get_len(value):
  ls=value.split(', ')
  return len(ls)


def dicc(row):
    #result = json.loads(row)
    result = ast.literal_eval(row)
    return result

def etl_atributtes(business):
    atributtes = pd.json_normalize(data = business['attributes'])
    atributtes['BusinessParking'].fillna("{'garage': False, 'street': False, 'validated': False, 'lot': False, 'valet': False}", inplace=True)
    atributtes.loc[atributtes['BusinessParking'] == 'None', 'BusinessParking'] = "{'garage': False, 'street': False, 'validated': False, 'lot': False, 'valet': False}"

    atributtes.loc[atributtes['Ambience'] == 'None', 'Ambience'] = "{'romantic': False, 'intimate': False, 'classy': False, 'hipster': False, 'touristy': False, 'trendy': False, 'upscale': False, 'casual': False}"
    atributtes['Ambience'] = atributtes['Ambience'].fillna("{'romantic': False, 'intimate': False, 'classy': False, 'hipster': False, 'touristy': False, 'trendy': False, 'upscale': False, 'casual': False}")

    

    garage = pd.json_normalize(atributtes.BusinessParking.apply(dicc))
    ambience = pd.json_normalize(atributtes['Ambience'].apply(dicc))

    atributtes['garage'] = garage['garage']
    atributtes['garage'].fillna(0, inplace=True)

    atributtes.drop(['BusinessParking', 'Ambience'], axis=1, inplace=True)

    ambience['good_ambience'] = ambience['romantic'] + ambience['intimate'] + ambience['classy'] + ambience['hipster'] + ambience['touristy'] + ambience['trendy'] + ambience['upscale'] + ambience['casual'] + ambience['divey']

    ambience.loc[ambience['good_ambience'] > 1, 'good_ambience'] = 1
    ambience.fillna(0, inplace=True)
    atributtes['good_ambience'] = ambience['good_ambience']

    atributtes['RestaurantsTakeOut'].fillna(0, inplace=True)
    atributtes['RestaurantsDelivery'].fillna(0, inplace=True)
    atributtes['RestaurantsTakeOut'] = atributtes['RestaurantsTakeOut'].map({'True': 1, 'False': 0, 'None': 0})
    atributtes['RestaurantsDelivery'] = atributtes['RestaurantsDelivery'].map({'True': 1, 'False': 0, 'None': 0})
    atributtes['delivery'] = atributtes['RestaurantsTakeOut'] + atributtes['RestaurantsDelivery']
    atributtes['delivery'] = pd.to_numeric(atributtes['delivery'], errors='coerce').fillna(0).astype(int)
    atributtes['delivery'] = atributtes['delivery'].replace(to_replace=2, value=1)


    atributtes.drop(['RestaurantsTakeOut', 'RestaurantsDelivery'], axis=1, inplace=True)

    top20 = atributtes.notna().sum().sort_values(ascending=False).head(20).index.tolist()

    atributtes = atributtes[top20]

    atributtes.fillna(0, inplace=True)
    atributtes.replace('None', 0, inplace=True)
    atributtes.replace('False', 0, inplace=True)
    atributtes.replace(False, 0, inplace=True)
    atributtes.replace('True', 1, inplace=True)

    atributtes['WiFi'] = np.where(atributtes['WiFi'] != 0, 1, 0)

    atributtes.loc[atributtes['Alcohol'] == "u'none'", 'Alcohol'] = 0
    atributtes.loc[atributtes['Alcohol'] == "'none'", 'Alcohol'] = 0
    atributtes.loc[atributtes['Alcohol'] != 0, 'Alcohol'] = 1
    atributtes['Alcohol'].unique()

    atributtes.loc[atributtes['NoiseLevel'].str.contains('quiet').fillna(False), 'NoiseLevel'] = 1
    atributtes.loc[atributtes['NoiseLevel'].str.contains('average').fillna(False), 'NoiseLevel'] = 2
    atributtes.loc[atributtes['NoiseLevel'].str.contains('loud').fillna(False), 'NoiseLevel'] = 3
    atributtes.loc[atributtes['RestaurantsAttire'] != 0,'RestaurantsAttire'] = 1
    atributtes.loc[atributtes['garage'] != 0,'garage'] = 1
    atributtes.loc[atributtes['GoodForMeal'] == 0,'GoodForMeal'] = "{'dessert': False, 'latenight': False, 'lunch': False, 'dinner': False, 'brunch': False, 'breakfast': False}"
    atributtes['GoodForMeal'].fillna("{'dessert': False, 'latenight': False, 'lunch': False, 'dinner': False, 'brunch': False, 'breakfast': False}", inplace=True)
    meal_types = pd.json_normalize(atributtes['GoodForMeal'].apply(dicc))
    atributtes.drop(['GoodForMeal'], axis=1, inplace=True)
    atributtes['meal_diversity'] = meal_types['dessert'] + meal_types['latenight'] + meal_types['lunch'] + meal_types['dinner'] + meal_types['brunch'] + meal_types['breakfast']
    atributtes['meal_diversity'].fillna(0, inplace=True)
    atributtes['business_id'] = business['business_id']
    return atributtes

def open_time(x):
    if pd.isnull(x):
        return None
    else:
        return x.split('-')[0]

def close_time(x):
    if pd.isnull(x):
        return None
    else:
        return x.split('-')[1]


def normalize_hours(lista:list, df):
    for i in lista:
        df.loc[pd.notnull(df[i]), i] = pd.to_datetime(df.loc[pd.notnull(df[i]), i]) - pd.to_datetime(df.loc[pd.notnull(df[i]), i]).dt.normalize()

def mean_open_hour(lista:list, df):

    for i in range(df.shape[0]):
        temp_sum = []
        for j in lista:
            if pd.notnull(df.loc[i, j]):
                temp_sum.append(df.loc[i, j])
        if len(temp_sum) > 0:
            df.loc[i, 'mean_open_hour'] = sum(temp_sum,datetime.timedelta())/len(temp_sum)

def mean_close_hour(lista:list, df):
    for i in range(df.shape[0]):
        temp_sum = []
        for j in lista:
            if pd.notnull(df.loc[i, j]):
                temp_sum.append(df.loc[i, j])
        if len(temp_sum) > 0:
            df.loc[i, 'mean_close_hour'] = sum(temp_sum,datetime.timedelta())/len(temp_sum)


def etl_hours(business):
    hours = pd.json_normalize(data = business['hours'])
    hours['business_id'] = business['business_id']
    subset = hours[~pd.isnull(hours[['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']]).any(1)].index
    hours['7days'] = 0
    hours.loc[subset, '7days'] = 1
    subset = hours[~pd.isnull(hours[['Friday', 'Saturday', 'Sunday']]).any(1)].index
    hours['weekends'] = 0
    hours.loc[subset, 'weekends'] = 1

    hours['monday_open_time'] = pd.to_datetime(hours['Monday'].apply(open_time), format='%H:%M')
    hours['monday_close_time'] = pd.to_datetime(hours['Monday'].apply(close_time), format='%H:%M')
    hours['monday_total_hours'] = (hours['monday_close_time'] - hours['monday_open_time'])/pd.Timedelta(hours=1)
    hours.loc[hours['monday_total_hours'] == 0.0, 'monday_total_hours'] = 24.0

    hours['tuesday_open_time'] = pd.to_datetime(hours['Tuesday'].apply(open_time), format='%H:%M')
    hours['tuesday_close_time'] = pd.to_datetime(hours['Tuesday'].apply(close_time), format='%H:%M')
    hours['tuesday_total_hours'] = (hours['tuesday_close_time'] - hours['tuesday_open_time'])/pd.Timedelta(hours=1)
    hours.loc[hours['tuesday_total_hours'] == 0.0, 'tuesday_total_hours'] = 24.0


    hours['wednesday_open_time'] = pd.to_datetime(hours['Wednesday'].apply(open_time), format='%H:%M')
    hours['wednesday_close_time'] = pd.to_datetime(hours['Wednesday'].apply(close_time), format='%H:%M')
    hours['wednesday_total_hours'] = (hours['wednesday_close_time'] - hours['wednesday_open_time'])/pd.Timedelta(hours=1)
    hours.loc[hours['wednesday_total_hours'] == 0.0, 'wednesday_total_hours'] = 24.0


    hours['thursday_open_time'] = pd.to_datetime(hours['Thursday'].apply(open_time), format='%H:%M')
    hours['thursday_close_time'] = pd.to_datetime(hours['Thursday'].apply(close_time), format='%H:%M')
    hours['thursday_total_hours'] = (hours['thursday_close_time'] - hours['thursday_open_time'])/pd.Timedelta(hours=1)
    hours.loc[hours['thursday_total_hours'] == 0.0, 'thursday_total_hours'] = 24.0


    hours['friday_open_time'] = pd.to_datetime(hours['Friday'].apply(open_time), format='%H:%M')
    hours['friday_close_time'] = pd.to_datetime(hours['Friday'].apply(close_time), format='%H:%M')
    hours['friday_total_hours'] = (hours['friday_close_time'] - hours['friday_open_time'])/pd.Timedelta(hours=1)
    hours.loc[hours['friday_total_hours'] == 0.0, 'friday_total_hours'] = 24.0


    hours['saturday_open_time'] = pd.to_datetime(hours['Saturday'].apply(open_time), format='%H:%M')
    hours['saturday_close_time'] = pd.to_datetime(hours['Saturday'].apply(close_time), format='%H:%M')
    hours['saturday_total_hours'] = (hours['saturday_close_time'] - hours['saturday_open_time'])/pd.Timedelta(hours=1)
    hours.loc[hours['saturday_total_hours'] == 0.0, 'saturday_total_hours'] = 24.0


    hours['sunday_open_time'] = pd.to_datetime(hours['Sunday'].apply(open_time), format='%H:%M')
    hours['sunday_close_time'] = pd.to_datetime(hours['Sunday'].apply(close_time), format='%H:%M')
    hours['sunday_total_hours'] = (hours['sunday_close_time'] - hours['sunday_open_time'])/pd.Timedelta(hours=1)
    hours.loc[hours['sunday_total_hours'] == 0.0, 'sunday_total_hours'] = 24.0

    lista_total_open_hours = ['monday_total_hours', 'tuesday_total_hours', 'wednesday_total_hours', 'thursday_total_hours', 'friday_total_hours', 'saturday_total_hours', 'sunday_total_hours']
    lista_open = ['monday_open_time', 'tuesday_open_time', 'wednesday_open_time', 'thursday_open_time', 'friday_open_time', 'saturday_open_time', 'sunday_open_time']
    lista_close = ['monday_close_time', 'tuesday_close_time', 'wednesday_close_time', 'thursday_close_time', 'friday_close_time', 'saturday_close_time', 'sunday_close_time']
    lista_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    lista_total =  lista_total_open_hours + lista_open + lista_close + lista_days

    hours['n_open_days'] = pd.notnull(hours[lista_open]).sum(axis=1)
    hours['mean_total_hours_open'] = hours[lista_total_open_hours].abs().mean(axis=1)

    normalize_hours(lista_open, hours)
    normalize_hours(lista_close, hours)
    mean_open_hour(lista_open, hours)
    mean_close_hour(lista_close, hours)
    hours.drop(lista_total, axis=1, inplace=True)
    return hours

def etl_categories(business):
    categories = business['categories'].str.split(', ', expand=True)
    categories.index = business['business_id']
    df = categories.T.stack().groupby('business_id').apply(list).reset_index(name='categories')
    mlb = MultiLabelBinarizer()
    df = df.join(pd.DataFrame(mlb.fit_transform(df.pop('categories')),
                            columns=mlb.classes_,
                            index=df.index))
    df['total_categories'] = df.sum(axis=1)
    most_frequent = ['Restaurants',
    'Food',
    'Shopping',
    'Home Services',
    'Beauty & Spas',
    'Nightlife',
    'Health & Medical',
    'Local Services',
    'Bars',
    'Automotive', 'total_categories', 'business_id']
    df = df.fillna(0)
    df = df[most_frequent]
    return df

def etl_gps(business):
    for_clustering = business[['latitude', 'longitude', 'business_id']]
    kmeans = KMeans(n_clusters = 11, init ='k-means++')
    Y_axis = for_clustering['latitude'].values.reshape(-1,1)
    X_axis = for_clustering['longitude'].values.reshape(-1,1)
    for_clustering.loc[:,'areas'] = kmeans.fit_predict(X_axis, Y_axis)
    return for_clustering


def get_len(value):
  """
  It takes a string of comma separated names and returns the number of names in the string
  
  :param value: the value of the column you're applying the function to
  :return: The number of friends in the list.
  """
  ls = value.split(', ')
  return len(ls)

