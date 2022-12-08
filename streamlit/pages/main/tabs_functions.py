import streamlit as st
import pandas as pd
from datetime import datetime
from multiprocessing import Value
from typing import List
from numpy.core.fromnumeric import size
# xtAuthProvider
#import joblib
import pickle
import xgboost
#import darts 
import plotly.express as px
import plotly.graph_objects as go
pd.options.plotting.backend = 'plotly'
from sqlalchemy import create_engine
# from darts import TimeSeries
# from darts.models import ExponentialSmoothing
# from darts.metrics import mape
from transformers import AlbertForSequenceClassification, pipeline, AlbertTokenizer
from keybert import KeyBERT
from Functions.Herramientas import ht 
#import pydeck as pdk
import numpy as np


###########################
## SOME USEFUL FUNCTIONS ###
###########################

def cql_to_pandas(cql_query,cassandra_session):
   """
   It takes a CQL query and a Cassandra session as input, and returns a Pandas dataframe
   
   :param cql_query: The CQL query you want to run
   :param cassandra_session: The Cassandra session object
   :return: A pandas dataframe
   """
   def pandaspark_factory(colnames, rows):\
       return pd.DataFrame(rows, columns=colnames)
   cassandra_session.row_factory = pandaspark_factory
   cassandra_session.default_fetch_size = None
   result = cassandra_session.execute(cql_query, timeout=None)
   return result._current_rows


def capitalize_each_word(original_str):
    result = ""
    # Split the string and get all words in a list
    list_of_words = original_str.split()
    # Iterate over all elements in list
    for elem in list_of_words:
        # capitalize first letter of each word and add to a string
        if len(result) > 0:
            result = result + " " + elem.strip().capitalize()
        else:
            result = elem.capitalize()
    # If result is still empty then return original string else returned capitalized.
    if not result:
        return original_str
    else:
        return result

engine = create_engine("mysql+pymysql://{user}:{pw}@{address}/{db}".format(user="root",
            address = '35.239.80.227:3306',
            pw="Henry12.BORIS99",
            db="yelp"))

def update_my_businesses(ids_list:list):
    tup = tuple(ids_list)
    if len(tup) > 1:
        business = pd.read_sql("""SELECT * FROM business_clean WHERE business_id in {}""".format(tup), engine)
        bus_names = business.name.to_list()
        for x in bus_names:
            users_business.add(x)
        checkin = pd.read_sql("""SELECT * FROM checkin_hour WHERE business_id in {}""".format(tup), engine)
        review = pd.read_sql("""SELECT * FROM review WHERE business_id in {}""".format(tup), engine, parse_dates=['date'])
        sentiment = pd.read_sql("""SELECT * FROM sentiment_by_business WHERE business_id in {}""".format(tup), engine)
        influencer_score = pd.read_sql("""SELECT * FROM business_target WHERE business_id in {}""".format(tup), engine)
    else:
        business = pd.read_sql("""SELECT * FROM business_clean WHERE business_id = '{}'""".format(tup[0]), engine)
        users_business.add(business.name[0])
        checkin = pd.read_sql("""SELECT * FROM checkin_hour WHERE business_id = '{}'""".format(tup[0]), engine)
        review = pd.read_sql("""SELECT * FROM review WHERE business_id = '{}'""".format(tup[0]), engine, parse_dates=['date'])
        sentiment = pd.read_sql("""SELECT * FROM sentiment_by_business WHERE business_id = '{}'""".format(tup[0]), engine)
        influencer_score = pd.read_sql("""SELECT * FROM business_target WHERE business_id = '{}'""".format(tup[0]), engine)
    return business, checkin, review, sentiment, influencer_score

#def update_cass_businesses(ids_list:list):
#    tup = tuple(ids_list)
#    import os
#    os.environ["JAVA_HOME"] = "/opt/java"
#    os.environ["SPARK_HOME"] = "/opt/spark"
#    import findspark
#    findspark.init()
#    from pyspark.sql import SparkSession
#    spark = SparkSession.builder.master("local[*]").getOrCreate()
#    import casspark
#    from cassandra.cluster import Cluster
#    cass_ip = '34.102.43.26'
#    cluster = Cluster(contact_points=[cass_ip],port=9042)
#    session = cluster.connect()
#
#    business = cql_to_pandas("""select * from yelp.business WHERE business_id in {} ALLOW FILTERING;""".format(bus_ids),session)
#    checkin = cql_to_pandas("""select * from yelp.checkin where business_id in {} ALLOW FILTERING;""".format(bus_ids),session)
#    review = cql_to_pandas("""select * from yelp.review where business_id in {} ALLOW FILTERING;""".format(bus_ids),session)
#    sentiment = cql_to_pandas("""SELECT * FROM yelp.sentiment_by_business WHERE business_id = '{}' ALLOW FILTERING""".format(bus_ids), engine)
#    influencer_score = cql_to_pandas("""SELECT * FROM yelp.business_target WHERE business_id = '{}' ALLOW FILTERING""".format(tup[0]), engine)
#    return business, checkin, review, sentiment, influencer_score


#users_business = ["Burger King", "Starbucks", "Subway", "Taco Bell", "CVS Pharmacy", "Acme Oyster House", "Michaelangelos Pizza", "Nana Rosa Italian"]
users_business = set()
bus_ids = ['vCJZ0WpB9r_tOhJZpESqCQ',
 'Ppy-UN5RptIog4AvNnAM-g',
 '4dW3dmdYRsfen8a2AtfZvg',
 '_ab50qdWOk0DdB6XOrBitw',
 'gLKNO0m4kLSqxWQKNJgMKg',
 '-QG6KSRQKTQ80--wqrnLTg',
 'pq7CAQGsxjaFcMLmhdbbvA',
 '2cfEAFZZee8fq3Xx6yZ87w',
 'HgvOxHGHnEway0hEn4jjtw']

##################
## IMPORT DATA ###
##################

using_cassandra = False

#try:
#business, checkin, review, sentiment, influencer_score = update_cass_businesses(bus_ids)
using_cassandra = True
# except:
business, checkin, review, sentiment, influencer_score = update_my_businesses(bus_ids)
#     using_cassandra = False

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

    ## Oportunities
    ## Gives you the top categories and locations of business according to probability predictions
    ## Of the XGBoost model 

    df = pd.read_csv('./pages/main/data/business_proba.csv').head(50)
    sucessfull_business = tuple(df['business_id'].unique().tolist())
    sucess_query = '''SELECT DISTINCT(postal_code), areas,
    restaurants, food, shopping, homeservices, beautyandspas, 
    nightlife, healthandmedical, localservices, bars, automotive 
    FROM yelp.business_clean where business_id IN {};'''
    df_sucess_zip = pd.read_sql(sucess_query.format(sucessfull_business), engine)
    df_sucess_zip['areas_name'] = df_sucess_zip['areas'].map({
        0: 'Philadelphia',
        1: 'Reno',
        2: 'Indianapolis',
        3: 'Tucson',
        4: 'New Orleans',
        5: 'St. Louis',
        6: 'Tampa',
        7: 'Boise',
        9: 'Nashville',
        10: 'Santa Barbara'
    })

    areas = df_sucess_zip['areas_name'].value_counts().head(3).index.tolist()
    areas = ', '.join(areas)

    subset = df_sucess_zip[['restaurants', 'food', 'shopping', 'homeservices', 'beautyandspas', 'nightlife', 'healthandmedical', 'localservices', 'bars', 'automotive']]
    business_lines = pd.get_dummies(subset).idxmax(1).value_counts().head(3).index.tolist()
    business_lines = ', '.join(business_lines)
    business_lines = capitalize_each_word(business_lines)
    
    st.markdown("### Oportunities")
    oportunity = st.columns(2)
    oportunity[0].metric('Hot Business Lines', business_lines, delta=None, delta_color="normal")
    oportunity[1].metric('Hot Locations', areas, delta=None, delta_color="normal")

    ## Account Summary
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
    sentiment1 = sentiment.loc[sentiment['business_id'].isin(ids)]
    checkin1 = checkin.loc[checkin['business_id'].isin(ids)]
    influencer_score['Influencer_score'] = 1 - (1 / (1 + influencer_score['avg(Influencer_2)']))
    inf_score_1 = influencer_score.loc[influencer_score['business_id'].isin(ids)]
    sentiment1['positive_score'] = sentiment1['pos_reviews'] / ( sentiment1['neg_reviews'] + sentiment1['pos_reviews'])
    Positive_sentiment = sentiment1['positive_score'].mean()
    review_total = review1.shape[0]
    number_visits = checkin1['total'].sum()   
    inf_score_1 = inf_score_1['Influencer_score'].mean()

    st.markdown("### Account Summary")

    metrics = st.columns(6)

    metrics[0].metric('Review Total',review_total, delta=None, delta_color="normal")
    metrics[1].metric('Review stars', round(review_stars, 2), delta=None, delta_color="normal")
    metrics[2].metric('Positive sentiment', f'{round(Positive_sentiment, 2)*100}%', delta=None, delta_color="normal")
    if len(checkin1) > 1:
        metrics[4].metric('Top Hour', f'{round(checkin1.avg_hour.mean())}:00', delta=None, delta_color="normal")
    elif len(checkin1) == 1:
        metrics[4].metric('Top Hour', f'{round(checkin1.avg_hour.iloc[0])}:00', delta=None, delta_color="normal")
    metrics[3].metric('Influencer Score', f'{round(inf_score_1, 2)*100}%', delta=None, delta_color="normal")
    metrics[5].metric('Number_visits', number_visits)
    #location = filtro[['latitude_x','longitude_x']]
    #ht.mapa3d(location)
    st.title('Sentiment Analysis for Last Reviews')
    number_to_get = st.slider('Number of reviews to get', 1, 50, 10)
    name = st.button('Analize reviews')
    if name:
        #reviews = get_reviews(id, engine, number_to_get)
        reviews = review1[['text', 'date']].sort_values(by='date', ascending=False).head(number_to_get)
        

        model = AlbertForSequenceClassification.from_pretrained('./model/textclass/')
        tokenizer = AlbertTokenizer.from_pretrained('./model/textclass/')
        classifier = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer) #, device=0) #for GPU support
        
        kw_model = KeyBERT()

        positive = 0
        negative = 0
        pos_keywords = []
        neg_keywords = []
        reviews['sentiment'] = ''
        reviews['keywords'] = ''
        for index, row in reviews.iterrows():
            if classifier(row['text'], truncation = True)[0]['label'] == 'LABEL_1':
                positive += 1
                keywords = kw_model.extract_keywords(row['text'], keyphrase_ngram_range=(1, 1), stop_words='english')
                pos_keywords += keywords
                reviews.sentiment[index] = 'positive'
                reviews.keywords[index] = keywords
            elif classifier(row['text'], truncation = True)[0]['label'] == 'LABEL_0':
                negative += 1
                keywords = kw_model.extract_keywords(row['text'], keyphrase_ngram_range=(1, 1), stop_words='english')
                neg_keywords += keywords
                reviews.sentiment[index] = 'negative'
                reviews.keywords[index] = keywords
        
        try:
            neg_key, neg_score = zip(*neg_keywords)
        except:
            neg_key = []
            neg_score = []
        try:
            pos_key, pos_score = zip(*pos_keywords)
        except:
            pos_key = []
            pos_score = []

        df_neg = pd.DataFrame({'key':neg_key, 'score':neg_score}).groupby('key').mean().sort_values('score', ascending=False)
        df_pos = pd.DataFrame({'key':pos_key, 'score':pos_score}).groupby('key').mean().sort_values('score', ascending=False)
            
        st.markdown("### Reviews Summary")
        metrics = st.columns(2)

        metrics[0].markdown("### Positive Reviews")
        metrics[0].metric('Total Positive', positive, delta=None, delta_color="normal")
        metrics[0].text("Top 5 Keywords")
        metrics[0].text(df_pos.head(5).index.tolist())

        metrics[1].markdown("### Negative Reviews")
        metrics[1].metric('Total Negative', negative, delta=None, delta_color="normal")
        metrics[1].text("Top 5 Keywords")
        metrics[1].text(df_neg.head(5).index.tolist())


        REVIEW_TEMPLATE_MD = """{} - {}
                                    > {}"""

        with st.expander("💬 Show Reviews"):

        # Show comments

            st.write("**Reviews:**")

            for index, entry in enumerate(reviews.itertuples()):
                st.markdown(REVIEW_TEMPLATE_MD.format(entry.date, entry.sentiment, entry.text))

def select_business():
    option = st.selectbox(
            'My businesses',
            users_business)

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
        'Tampa',
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
        elif area == 'Reno':
            df_4['areas_1'] = 1.0
        elif area == 'Indianapolis':
            df_4['areas_2'] = 1.0
        elif area == 'Tucson':
            df_4['areas_3'] = 1.0
        elif area == 'New Orleans':
            df_4['areas_4'] = 1.0
        elif area == 'St. Louis':    
            df_4['areas_5'] = 1.0
        elif area == 'Tampa':
            df_4['areas_6'] = 1.0
        elif area == 'Boise':
            df_4['areas_7'] = 1.0
        elif area == 'Nashville':
            df_4['areas_9'] = 1.0
        elif area == 'Santa Barbara':
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

        #print(df_predictions_final)
        
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
    fig1.update_traces(line_color='purple', name='Actual', x='Years', y='Total Reviews')


    fig2 = px.line(forecast.pd_dataframe())
    fig2.update_layout(title='Forecast')
    fig2.update_traces(line_color='seagreen', name='Forecast')

    fig = go.Figure(data = fig1.data + fig2.data)
    return fig, string



####### NO BORRAR #######

# def timeseries():
#     df = pd.read_csv('pages/main/data/forecasting.csv', parse_dates=['month'], index_col='month')
#     df = df['2010':]

#     # st.title('Time Series Visualization')
#     st.markdown('Reviews/Tips/Checkins by Month for the Top Brands in USA'
#     )

#     # Create a list of unique brands
#     st.text("Select you favourite brand")
#     top_brand_selected = st.multiselect('Select brand', df.columns.tolist(), df.columns.tolist()[0:3])

#     st.plotly_chart(df[top_brand_selected].plot(title = 'Total Review/Tips/Checkins Counts on Yelp for Top Brands'))


#     series = TimeSeries.from_dataframe(df, fill_missing_dates=True, freq='MS', fillna_value=0)

#     st.title('Forecasting Time Series')
#     st.markdown('Reviews/Tips/Checkins by Month for the Top Brands in USA'
#     )

#     # Create a list of unique brands
#     st.text("Select you favourite brand")
#     top_brand_selected_f = st.selectbox('Select brand for forecast', df.columns.tolist())

#     train, val = series[top_brand_selected_f].split_after(pd.Timestamp('2021-01-01'))
    
#     for m in range(2, 25):
#         is_seasonal, mseas = check_seasonality(train, m=m, alpha=0.05)
#         if is_seasonal:
#             break
    
    
    model = ExponentialSmoothing()

    fig, string = eval_model(model, train, val)

    fig.update_layout(title=top_brand_selected_f)
    st.plotly_chart(fig)

    st.text(string)

def timeseries():
    train_df = pd.read_csv('./pages/main/data/train_ts.csv', index_col='month', parse_dates=True)
    forecast_df = pd.read_csv('./pages/main/data/forecast_ts.csv', index_col='month', parse_dates=True)
    type_model = pd.read_csv('./pages/main/data/model_for_each_ts.csv', index_col=0)
    #print(type_model)

    st.title('Time Series Visualization')
    st.markdown('Reviews/Tips/Checkins by Month for the Top Brands in USA')

    st.text("Select you favourite brand")
    top_brand_selected = st.multiselect('Select brand', train_df.columns.tolist(), train_df.columns.tolist()[0:3])

    st.plotly_chart(train_df[top_brand_selected].plot(title = 'Total Review/Tips/Checkins Counts on Yelp for Top Brands'))

    st.title('Forecasting Time Series')
    st.markdown('Reviews/Tips/Checkins by Month for the Top Brands in USA')

    st.text("Select you favourite brand")
    top_brand_selected_f = st.multiselect('Select brand for forecast', train_df.columns.tolist(), train_df.columns.tolist()[0:2])

    make_forecast = st.button('Make Forecasts')

    if make_forecast:
        for i in top_brand_selected:
            fig1 = px.line(train_df[i])
            fig1.update_layout(title='Actual')
            fig1.update_traces(line_color='purple', name='Actual')

            fig2 = px.line(forecast_df[i])
            fig2.update_layout(title='Forecast')
            fig2.update_traces(line_color='seagreen', name='Forecast')

            fig = go.Figure(data = fig1.data + fig2.data)
            fig.update_layout(title=i)
            st.plotly_chart(fig)
            texto = 'Type of model used for {}: {}'.format(i, type_model.loc[i, 'Best Model'])
            htmlcode = "<p style='text-align: center; color: red;'>{}</p>".format(texto)
            st.markdown(htmlcode, unsafe_allow_html=True)



############################################## Add business ############################################


def addbusiness():
    global business, checkin, review, sentiment, influencer_score, using_cassandra
    st.markdown('#### Add your bussiness name')
    name = st.text_input('Add your business name 👇 and hit ENTER', '')
    if name != None:
        df = pd.read_sql('SELECT address, postal_code FROM business_clean WHERE name = "{}" ORDER BY postal_code ASC'.format(name), con=engine)

        st.markdown('#### Postal Code')
        zipcode = st.selectbox('Select your postal code 👇', df['postal_code'].unique().tolist())

        st.markdown('#### Address')
        address = st.selectbox('Select your address 👇', df.loc[df['postal_code'] == zipcode,'address'].unique().tolist())

        add_my_bussiness = st.button('Add my business')

        if add_my_bussiness:
            new_business_id = pd.read_sql('SELECT business_id FROM business_clean WHERE name = "{}" AND postal_code = "{}" AND address = "{}"'.format(name, zipcode, address), con=engine)['business_id'].values[0]
            #print(f'ADDING NEW BUSINESS: {new_business_id}')
            bus_ids.append(new_business_id)
            #print(bus_ids)
            users_business.add(capitalize_each_word(name))
            #print(users_business)
            st.text('Your business id is: {}'.format(new_business_id))
            st.text('Business added to dashboard successfully')
            #if using_cassandra:
            new_business, new_checkin, new_review, new_sentiment, new_influencer_score = update_cass_businesses([new_business_id])
            # else:
            #     new_business, new_checkin, new_review, new_sentiment, new_influencer_score = update_my_businesses([new_business_id])
            business = pd.concat([business, new_business], axis=0)
            checkin = pd.concat([checkin, new_checkin], axis=0)
            review = pd.concat([review, new_review], axis=0)
            sentiment = pd.concat([sentiment, new_sentiment], axis=0)
            influencer_score = pd.concat([influencer_score, new_influencer_score], axis=0)

    else:
        st.text('Business not found, check the name and try again')

def dataframe():
    df = pd.read_sql("""SELECT bc.name,bc.address,bc.longitude_x,bc.latitude_x,bt.success_score FROM business_clean  bc join business_target bt on bc.business_id=bt.business_id limit 1000""", engine)
    return df
    #'address','latitude_x','longitude_x','success_score'

data1 = dataframe()



def mapa3dGrafico():
    #dicc = {'latitude_x':[37.742699],'longitude_x':[-122.392366],'name':['pepito'],'address':['EE.UU']}
    #df =pd.DataFrame(dicc)
    #df = pd.DataFrame(np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],columns=['latitude_x','longitude_x'])
    data2 = data1.sort_values(by='success_score',ascending=False)
    ht.mapa3d(data2)
    