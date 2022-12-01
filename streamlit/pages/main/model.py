import streamlit as st
import pandas as pd 
import numpy as np
from multiprocessing import Value
from typing import List
#import joblib
#from prediction import predict
import pickle
import xgboost

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
