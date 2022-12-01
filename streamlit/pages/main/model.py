import streamlit as st
import pandas as pd 
import numpy as np
from multiprocessing import Value
from typing import List
import joblib
#from prediction import predict

def predict(data):
    clf = joblib.load('../../model/xgb_business.joblib')
    return clf.predict(data)


def machine_learning():
    st.markdown('Model to classify business into popular vs. no popular')

    st.header("Business features")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.text("Geographic location")
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

        st.text("Type of business")
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

        
        st.text("Price range")
        price_range = st.slider('Price range', min_value = 0, max_value = 4, value = 1)

        st.text("Noise level")
        noise_level = st.slider('Noise level', min_value = 0, max_value = 4, value = 1)

    with col3: 
        st.text("Open times")

        weekends = st.checkbox(
        "Open on weekends",
        help="Weekends mean friday, saturday and sundays")

        open_hours = st.slider('Total open hours per day', min_value = 0, max_value = 24, value = 1)


    st.header("Additional features")
    col1, col2, col3 = st.columns(3)

    with col1:
        ambience = st.checkbox(
        "Good ambience",
        help="Good ambience")

        good_for_groups = st.checkbox(
        "Good for groups",
        help="Good for groups")

        good_for_kids = st.checkbox(
        "Good for kids",
        help="Good for kids")
        
        has_tv = st.checkbox(
        "Has TV",
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
        help="Bike parking")

        credit_cards = st.checkbox(
        "Credit cards",
        help="Credit cards")



    with col3:
        caters = st.checkbox(
        "Caters",
        help="Caters")

        elegancy = st.checkbox(
        "Elegant place / formal",
        help="Elegancy")

        by_appointment_only = st.checkbox(
        "By appointment only",
        help="By appointment only")

        wifi = st.checkbox(
        "Wifi",
        help="Wifi")  


    if st.button('Predict Business Success'):

        data = pd.DataFrame({
            'ambience': [ambience],
            'garage': [garage],
            'credit_cards': [credit_cards],
            'price_range': [price_range],
            'bike_parking': [bike_parking],
            'area': [area],
            'type_of_business': [type_of_business],
            
            'noise_level': [noise_level],
            'weekends': [weekends],
            'open_hours': [open_hours],
            
            'good_for_groups': [good_for_groups],
            'good_for_kids': [good_for_kids],
            'has_tv': [has_tv],
            'outdoor_seating': [outdoor_seating],
            'alcohol': [alcohol],
            'delivery': [delivery],
            
            
            
            'caters': [caters],
            'elegancy': [elegancy],
            'by_appointment_only': [by_appointment_only],
            'wifi': [wifi]
        })

        print(data)

        #prediction = predict(data)

        if 1 == 1:
            st.success('Business is popular')
        else:
            st.error('Business is not popular')
