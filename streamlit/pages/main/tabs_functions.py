import streamlit as st
import pandas as pd 
import numpy as np
from multiprocessing import Value
from os import write
from typing import List
from numpy.core.fromnumeric import size
from PIL import Image
# xtAuthProvider
import json
from pathlib import Path
#import joblib
import pickle
import xgboost
#import darts 
from darts import TimeSeries
from darts.models import ExponentialSmoothing
from darts.metrics import mape
import plotly as py
import plotly.express as px
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.graph_objects as go
pd.options.plotting.backend = 'plotly'


##################
## IMPORT DATA ###
##################


business = pd.read_json(r'pages/main/data/my_business.json', lines=True)
checkin = pd.read_json(r'pages/main/data/my_checkins.json', lines=True)
review = pd.read_json(r'pages/main/data/my_reviews.json', lines=True)
sentiment = pd.read_json(r'pages/main/data/my_sent.json', lines=True)
influencer_score = pd.read_csv(r'C:\Users\USER\Documents\SOYHENRY\LABS\TRABAJO_GRUPAL\trabajofinal\streamlit\pages\main\data\target_3_influencer_modified.csv')

#business = cql_to_pandas("""select * from yelp.business ALLOW FILTERING;""",session)

import gzip
import shutil
with gzip.open(r'pages/main/data/my_user.json.gz', 'rb') as f_in:
    with open(r'pages/main/data/my_user.json', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

users = pd.read_json(r'pages/main/data/my_user.json', lines=True)

# im = Image.open(r'image/logo_vocado.png')
#business = cql_to_pandas("""select * from yelp.business ALLOW FILTERING;""",session)
#checkin = cql_to_pandas("""select * from yelp.checkin ALLOW FILTERING;""",session)
#review = cql_to_pandas("""select * from yelp.review ALLOW FILTERING;""",session)
# review = pd.read_csv(r'pages/main/data/review_1000.csv')
# checkin = pd.read_csv(r'pages/main/data/checkin_1000.csv')


# cloud_config= {'secure_connect_bundle': r'secure-connect-henry.zip'}
# auth_provider = PlainTextAuthProvider(json.load(open(r'log_in.json'))['log_user'], json.load(open(r'log_in.json'))['log_password'])
# cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
# session = cluster.connect()

# def cql_to_pandas(cql_query,cassandra_session):
#     """
#     It takes a CQL query and a Cassandra session as input, and returns a Pandas dataframe
    
#     :param cql_query: The CQL query you want to run
#     :param cassandra_session: The Cassandra session object
#     :return: A pandas dataframe
#     """
#     def pandaspark_factory(colnames, rows):\
#         return pd.DataFrame(rows, columns=colnames)
#     cassandra_session.row_factory = pandaspark_factory
#     cassandra_session.default_fetch_size = None
#     result = cassandra_session.execute(cql_query, timeout=None)
#     return result._current_rows

users_business = ["Burger King", "Starbucks", "Subway", "Taco Bell", "CVS Pharmacy", "Acme Oyster House", "Michaelangelos Pizza", "Nana Rosa Italian"]

# business = cql_to_pandas("""select * from yelp.business_full where name in {} ALLOW FILTERING;""".format(tuple(users_business)),session)
# bus_ids = business.business_id.to_list()
# checkin = cql_to_pandas("""select * from yelp.checkin_full where business_id in {} ALLOW FILTERING;""".format(tuple(bus_ids)),session)
# reviews = cql_to_pandas("""select * from yelp.review_full where business_id in {} ALLOW FILTERING;""".format(tuple(bus_ids)),session)

############################################ HOME TAB ##################################################


def metricas(): 
   review_stars = business['stars'].mean()
   sentiment['positive_score'] = sentiment['pos_reviews'] / ( sentiment['neg_reviews'] + sentiment['pos_reviews'])
   Positive_sentiment = sentiment['positive_score'].mean()
   #review_total = sentiment.shape[0]
   review_total = review.shape[0]
   number_visits = checkin['total'].sum()
   influencer_score['Influencer_score'] = 1 - (1 / (1 + influencer_score['avg(Influencer_2)']))
   inf_score = influencer_score['Influencer_score'].mean()

   st.markdown("### Oportunities")
   oportunity = st.columns(3)
   oportunity[0].metric('Business Line', '98,7%', delta=None, delta_color="normal")
   oportunity[1].metric('Location', '93,5%', delta=None, delta_color="normal")
   oportunity[2].metric('Services', '90,7%', delta=None, delta_color="normal")

   st.markdown("### Account Summary")
   metrics = st.columns(6)
   metrics[0].metric('Review Total', review_total, delta=None, delta_color="normal")
   # metrics[1].image(im,width=50)
   metrics[1].metric('Review stars', round(review_stars, 2), delta=None, delta_color="normal")
   metrics[2].metric('Positive sentiment', f'{round(Positive_sentiment, 2)*100}%', delta=None, delta_color="normal")
   metrics[3].metric('Influencer Score', f'{round(inf_score, 2)*100}%', delta=None, delta_color="normal")
   metrics[4].metric('Top Hour', '18:00', delta=None, delta_color="normal")
   metrics[5].metric('Number_visits', number_visits)



############################################ BUSINESS TAB ##################################################




def query_info(filtro):
   ids = filtro['business_id'].to_list()
   
   review1 = review.loc[review['business_id'].isin(ids)]
   review_stars = filtro['stars'].mean()
   #review = cql_to_pandas("""select * from yelp.sentiment_business_full where business_id in '{}' ALLOW FILTERING;""".format(ids),session)
   sentiment1 = sentiment.loc[sentiment['business_id'].isin(ids)]
   #checkin = cql_to_pandas("""select * from yelp.checkin_full where business_id in '{}' ALLOW FILTERING;""".format(ids),session)
   checkin1 = checkin.loc[checkin['business_id'].isin(ids)]
   influencer_score['Influencer_score'] = 1 - (1 / (1 + influencer_score['avg(Influencer_2)']))
   inf_score_1 = influencer_score.loc[influencer_score['business_id'].isin(ids)]

   sentiment1['positive_score'] = sentiment1['pos_reviews'] / ( sentiment1['neg_reviews'] + sentiment1['pos_reviews'])
   Positive_sentiment = sentiment1['positive_score'].mean()
   #review_total = sentiment1.shape[0]
   review_total = review1.shape[0]
   number_visits = checkin1['total'].sum()   
   inf_score_1 = inf_score_1['Influencer_score'].mean()

   st.markdown("### Account Summary")
   
   metrics = st.columns(6)
   
   metrics[0].metric('Review Total',review_total, delta=None, delta_color="normal")
   metrics[1].metric('Review stars', round(review_stars, 2), delta=None, delta_color="normal")
   metrics[2].metric('Positive sentiment', f'{round(Positive_sentiment, 2)*100}%', delta=None, delta_color="normal")
   metrics[3].metric('Influencer Score', f'{round(inf_score_1, 2)*100}%', delta=None, delta_color="normal")
   metrics[4].metric('Top Hour', '18:00', delta=None, delta_color="normal")
   metrics[5].metric('Number_visits', number_visits)
        
def select_business(): 
    option = st.selectbox(
        'My businesses',
        (users_business))

    #business = cql_to_pandas("""select * from yelp.business_full where business_id = '{}' ALLOW FILTERING;""".format(option),session)

    # st.write('You selected:', option)
    if option in option:
        filtro = business[business['name'] == option]
        query_info(filtro)





##################################### MODEL TAB ##################################################





# def predict(data):
#     #Refers to path of main.py
#     #clf = joblib.load('./model/xgb_business.joblib')
#     clf = pickle.load(open('./model/xgb_business.pkl', "rb"))
#     return clf.predict(data)


def machine_learning():
    st.markdown("Discover which business lines, location and services get you the better chances at being successful (Based on popularity)")

    st.header("Business Features")
    col1, col2, col3 = st.columns(3)

    with col1:
        # st.text("Geographic location")
        area = st.selectbox('Select geographical area', [
        'Philadelphia', 
        'Indianapolis', 
        'St. Louis', 
        'Nashville', 
        'New Orleans', 
        'Orlando',
        'Tucson', 
        'Santa Barbara', 
        'Reno', 
        'Boise'])

        # st.text("Type of business")
        type_of_business = st.selectbox('Select type of business', [
            'Restaurant', 
            'Food', 
            'Nightlife', 
            'Shopping', 
            'Beauty & Spas', 
            'Bars',
            'Automotive', 
            'Health & Medical', 
            'Home Services', 
            'Local Services', 
            'Other'])

    with col2:
        # st.text("Price range")
        price_range = st.slider('Price range', min_value = 0, max_value = 4, value = 1)

        # st.text("Noise level")
        noise_level = st.slider('Noise level', min_value = 0, max_value = 4, value = 1)

    with col3: 
        # st.text("Open times")
        meal_diversity = st.slider('Meal diversity (if restaurant)', min_value = 0, max_value = 6, value = 1,
        help = "Meal diversity, 1 being only breakfast or dinner, 6 being all meals")

        open_hours = st.slider('Open Hours', min_value = 0.0, max_value = 24.0, value = 1.0, help= "Total open hours per day")
        
        weekends = st.checkbox(
        "Open on weekends",
        help="Weekends mean friday, saturday and sundays")

    st.header("Additional Features")
    col1, col2, col3 = st.columns(3)

    with col1:
        ambience = st.checkbox(
        "Good ambience",
        help="Comfortable, clean, peaceful, etc.")

        good_for_groups = st.checkbox(
        "Good for groups",
        help="Offers space for groups allocation")

        good_for_kids = st.checkbox(
        "Good for kids",
        help="Offers space for kids entertainment")
        
        has_tv = st.checkbox(
        "TV",
        help="Has TV")

        outdoor_seating = st.checkbox(
        "Outdoor seating",
        help="Outdoor seating")


    with col2:
        alcohol = st.checkbox(
        "Alcohol",
        help="Alcohol")

        delivery = st.checkbox(
        "Delivery",
        help="Delivery")

        garage = st.checkbox(
        "Garage",
        help="Garage")

        bike_parking = st.checkbox(
        "Bike parking",
        help="Offers parking locations for bikes")

        credit_cards = st.checkbox(
        "Credit cards",
        help="Accept credit cards")

    with col3:
        caters = st.checkbox(
        "Caters",
        help="Provides food service at a remote site")

        elegancy = st.checkbox(
        "Elegant",
        help="Provide elegant or formal ambience")

        by_appointment_only = st.checkbox(
        "Appointment",
        help="By appointment only")

        wifi = st.checkbox(
        "Wifi",
        help="Has Wifi")  

        reservations = st.checkbox(
        "Accept reservations",
        help="Accept reservations prior to attendance")  

    if st.button('Predict Business Success'):

        areas_name = [
        'Philadelphia', 
        'Indianapolis', 
        'St. Louis', 
        'Nashville', 
        'New Orleans', 
        'Orlando',
        'Tucson', 
        'Santa Barbara', 
        'Reno', 
        'Boise']


        df_1 = pd.DataFrame({
            'ambience': [ambience],
            'garage': [garage],
            'credit_cards': [credit_cards],
            'bike_parking': [bike_parking],
            'wifi': [wifi],
            'delivery': [delivery],
            'good_for_kids': [good_for_kids],
            'outdoor_seating': [outdoor_seating],
            'reservations': [reservations],
            'has_tv': [has_tv],
            'good_for_groups': [good_for_groups],
            'alcohol': [alcohol],
            'by_appointment_only': [by_appointment_only],
            'caters': [caters],
            'elegancy': [elegancy],
            'noise_level': [noise_level],
            'meal_diversity': [meal_diversity]
        })

        df_1 = df_1.astype(int)

        df_2 = pd.DataFrame({   'Restaurants': [0.0],
                                                'Food': [0.0],
                                                'Shopping': [0.0],
                                                'Home Services': [0.0],
                                                'Beauty & Spas': [0.0],
                                                'Nightlife': [0.0],
                                                'Health & Medical': [0.0],
                                                'Local Services': [0.0],
                                                'Bars': [0.0],
                                                'Automotive': [0.0]})

        if type_of_business == 'Restaurant':
            df_2['Restaurants'] = 1.0
        elif type_of_business == 'Food':
            df_2['Food'] = 1.0
        elif type_of_business == 'Shopping':
            df_2['Shopping'] = 1.0
        elif type_of_business == 'Home Services':
            df_2['Home Services'] = 1.0
        elif type_of_business == 'Beauty & Spas':
            df_2['Beauty & Spas'] = 1.0
        elif type_of_business == 'Nightlife':
            df_2['Nightlife'] = 1.0
        elif type_of_business == 'Health & Medical':
            df_2['Health & Medical'] = 1.0
        elif type_of_business == 'Local Services':
            df_2['Local Services'] = 1.0
        elif type_of_business == 'Bars':
            df_2['Bars'] = 1.0
        elif type_of_business == 'Automotive':
            df_2['Automotive'] = 1.0

        df_3 = pd.DataFrame({'weekends': [int(weekends)],
                            'open_hours': [float(open_hours)]})


        df_4 = pd.DataFrame({   'areas_0': [0.0],
                                    'areas_1': [0.0],
                                    'areas_2': [0.0],
                                    'areas_3': [0.0],
                                    'areas_4': [0.0],
                                    'areas_5': [0.0],
                                    'areas_6': [0.0],
                                    'areas_8': [0.0],
                                    'areas_9': [0.0],
                                    'areas_10': [0.0]})

        if area == 'Philadelphia':
            df_4['areas_0'] = 1.0
        elif area == 'Indianapolis':
            df_4['areas_1'] = 1.0
        elif area == 'St. Louis':    
            df_4['areas_2'] = 1.0
        elif area == 'Nashville':
            df_4['areas_3'] = 1.0
        elif area == 'New Orleans':
            df_4['areas_4'] = 1.0
        elif area == 'Orlando':
            df_4['areas_5'] = 1.0
        elif area == 'Tucson':
            df_4['areas_6'] = 1.0
        elif area == 'Santa Barbara':
            df_4['areas_8'] = 1.0
        elif area == 'Reno':
            df_4['areas_9'] = 1.0
        elif area == 'Boise':
            df_4['areas_10'] = 1.0

        df_5 = pd.DataFrame({   'price_ranges_0': [0.0],
                                            'price_ranges_1': [0.0],
                                            'price_ranges_2': [0.0],
                                            'price_ranges_3': [0.0],
                                            'price_ranges_4': [0.0]})
        if price_range == 0:
            df_5['price_ranges_0'] = 1.0
        elif price_range == 1:
            df_5['price_ranges_1'] = 1.0
        elif price_range == 2:
            df_5['price_ranges_2'] = 1.0
        elif price_range == 3:
            df_5['price_ranges_3'] = 1.0
        elif price_range == 4:
            df_5['price_ranges_4'] = 1.0

        df_predictions_final = pd.concat([df_1, df_2, df_3, df_4, df_5], axis = 1)
        
        clf = pickle.load(open('./model/xgb_business.pkl', 'rb'))

        df_predictions_final.columns = clf.get_booster().feature_names

        print(df_predictions_final)
        
        prediction = clf.get_booster().predict(xgboost.DMatrix(df_predictions_final))
        
        st.success('Business probability of success: {prediction:.2f}'.format(prediction = prediction[0]))

        if prediction > 0.5:
            st.success('Business is popular')
        else:
            st.error('Business is not popular')




############################################## TIME SERIES TAB ############################################




def eval_model(model, train, val):
    model.fit(train)
    forecast = model.predict(len(val))
    
    string = "model {} obtains MAPE: {:.2f}%".format(model, mape(val, forecast))

    fig1 = px.line(train.pd_dataframe())
    fig1.update_layout(title='Actual')
    fig1.update_traces(line_color='purple', name='Actual')


    fig2 = px.line(forecast.pd_dataframe())
    fig2.update_layout(title='Forecast')
    fig2.update_traces(line_color='seagreen', name='Forecast')

    fig = go.Figure(data = fig1.data + fig2.data)
    return fig, string

def timeseries():

    df = pd.read_csv('pages/main/data/forecasting.csv', parse_dates=['month'], index_col='month')
    df = df['2010':]

    # st.title('Time Series Visualization')
    st.markdown('Reviews/Tips/Checkins by Month for the Top Brands in USA'
    )

    # Create a list of unique brands
    st.text("Select you favourite brand")
    top_brand_selected = st.selectbox('Select brand', df.columns.tolist())

    st.plotly_chart(df[top_brand_selected].plot(title = 'Total Review/Tips/Checkins Counts on Yelp for Top Brands'))

    series = TimeSeries.from_dataframe(df, fill_missing_dates=True, freq='MS', fillna_value=0)

    st.title('Forecasting Time Series')
    st.markdown('Reviews/Tips/Checkins by Month for the Top Brands in USA'
    )

    # Create a list of unique brands
    st.text("Select you favourite brand")
    top_brand_selected_f = st.selectbox('Select brand for forecast', df.columns.tolist())

    train, val = series[top_brand_selected_f].split_after(pd.Timestamp('2021-01-01'))
    
    model = ExponentialSmoothing()

    fig, string = eval_model(model, train, val)

    fig.update_layout(title=top_brand_selected_f)
    st.plotly_chart(fig)

    st.text(string)



# def forecasting():

#     df = pd.read_csv('./data/forecasting.csv', parse_dates=['month'], index_col='month')
#     df = df['2010':]
