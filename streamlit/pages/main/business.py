import streamlit as st
import pandas as pd 
from multiprocessing import Value
from typing import List

def selete_business(): 
    business = pd.read_csv(r'business_1000.csv')
    name_business = business['name']

    option = st.selectbox(
        'My businesses',
        (name_business.to_list()))

    st.write('You selected:', option)

