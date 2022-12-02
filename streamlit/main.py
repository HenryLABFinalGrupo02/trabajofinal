import streamlit as st
import pandas as pd 
from streamlit_option_menu import option_menu
from multiprocessing import Value
from pages.main import tabs_functions as tf
from PIL import Image

st.set_page_config(
   page_title="Vocado",
   page_icon="🥑",  
   layout="wide", 
   menu_items = {
         'Get Help': 'https://github.com/HenryLABFinalGrupo02/trabajofinal',
         'Report a bug': "https://github.com/HenryLABFinalGrupo02/trabajofinal",
         'About': "# This is a header. This is an *VOCADO* cool app!"})

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


##################
###### MENU ######
##################

with st.sidebar:
   st.image(Image.open('./image/logo_vocado (5).png'))

   selected2 = option_menu(None, ["Home", "My Business", "Competition", "Opportunities", "Settings", "Add business"], 
   icons=['house', 'building', 'globe', 'star', 'gear', 'plus'], 
   menu_icon="cast", default_index=0, orientation="vertical",
   styles={
        "container": {"padding": "0!important", 
                     "background-color": "#E4FFED"},
        "icon": {"color": "#F4C01E",
                  "font-size": "25px"}, 
        "nav-link": {"font-size": "25px", 
                     "margin":"0px", 
                     "--hover-color": "#109138", 
                     "font-family":"Sans-serif", 
                     "background-color": "#E4FFED"},
        "nav-link-selected": {"background-color": "#109138", 
                              "font-style":"Sans-serif", 
                              "font-weight": "bold",
                              "color":"#FFFFFF"},
    })

#####################
## IMPORT FUNTIONS ##
#####################

## HOME 
if selected2 == "Home":
   st.title('Welcome to Vocado Admin Center')
   tf.metricas()

## My Business
if selected2 == "My Business":
   st.title('Business Admin Center')
   tf.select_business()

## My Competition
if selected2 == "Competition":
   st.title('Competition')
   tf.timeseries()

## My Opportunities
if selected2 == "Opportunities":
   st.title('Opportunities Exploration')
   tf.machine_learning()
 
# ## Time Series Analysis
# if selected2 == "Time Series Analysis":
#    st.title('Time Series Analysis')
#    tf.timeseries()



