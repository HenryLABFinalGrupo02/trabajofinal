import streamlit as st
import pandas as pd 
from streamlit_option_menu import option_menu
from multiprocessing import Value
from pages.main import home as m
from pages.main import business as p
<<<<<<< HEAD
from pages.main import model as ml
=======
from PIL import Image

##################
## PAGE CONFIG ###
##################
>>>>>>> a56658d55ed1db92a8945702d088064c12a49d91

st.set_page_config(
   page_title="Vocado",
   page_icon="🥑",  
   layout="wide", 
   menu_items = {
         'Get Help': 'https://github.com/HenryLABFinalGrupo02/trabajofinal',
         'Report a bug': "https://github.com/HenryLABFinalGrupo02/trabajofinal",
         'About': "# This is a header. This is an *VOCADO* cool app!"}
)

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

##################
## IMPORT DATA ###
##################

business = pd.read_csv(r'business_1000.csv')
checkin = pd.read_csv(r'checkin_1000.csv')
review = pd.read_csv(r'review_1000.csv')
#tip = pd.read_csv(r'C:\Users\USER\Documents\SOYHENRY\LABS\TRABAJO_GRUPAL\trabajofinal\Airflow-Spark\data\tip_1000.csv')
#user = pd.read_csv(r'C:\Users\USER\Documents\SOYHENRY\LABS\TRABAJO_GRUPAL\trabajofinal\Airflow-Spark\data\user_1000.csv')

##################
###### MENU ######
##################

with st.sidebar:
   st.image(Image.open('logo_vocado.png'))

   selected2 = option_menu(None, ["Home", "My Business", "Competition", "Opportunities", "Settings", "Add business"], 
   icons=['house', 'building', 'globe', 'star', 'gear', 'plus'], 
   menu_icon="cast", default_index=0, orientation="vertical",
   styles={
        "container": {"padding": "0!important", 
                     "background-color": "#109138"},
        "icon": {"color": "#F4C01E",
                  "font-size": "25px"}, 
        "nav-link": {"font-size": "25px", 
                     "margin":"0px", 
                     "--hover-color": "#16C64D", 
                     "font-family":"Sans-serif"},
        "nav-link-selected": {"background-color": "#16C64D", 
                              "font-style":"Sans-serif", 
                              "font-weight": "bold",
                              "color":"#121212"},
    })

#####################
## IMPORT FUNTIONS ##
#####################

## HOME 
if selected2 == "Home":
   st.title('Welcome to Vocado Admin Center')
   m.metricas()

## My Business
if selected2 == "My Business":
   st.title('Business Admin Center')
   p.select_business()

## My Competition
if selected2 == "Competition":
   st.title('Competition Admin Center')

## My Opportunities
if selected2 == "Opportunities":

   st.title('Opportunities Admin Center')

   ml.machine_learning()
 
