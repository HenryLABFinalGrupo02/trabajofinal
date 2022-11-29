import streamlit as st
import pandas as pd 
from streamlit_option_menu import option_menu
from cgitb import text
from multiprocessing import Value
from os import write
from turtle import onclick, onscreenclick
from typing import List
from numpy.core.fromnumeric import size

st.set_page_config(
   page_title="Project Henry",
   page_icon="👋",  
   layout="wide"
)

st.title('Welcome to Vocado Admin Center')

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

# st.sidebar.title('Menu')
# paginas = st.sidebar.selectbox("Select :",['pagina1','pagina2'])

# with st.sidebar:
#         tabs = on_hover_tabs(tabName=['Home', 'Business #1', 'Business #2'], 
#                              iconName=['home', 'business', 'business'],
#                              styles = {'navtab': {'background-color':'transparent',
#                                                   'color': '#FFFFFF',
#                                                   'font-size': '18px',
#                                                   'transition': '.3s',
#                                                   'white-space': 'nowrap',
#                                                   'text-transform': 'uppercase'},
#                                        'tabOptionsStyle': {':hover :hover': {'color': '#E4FFED',
#                                                                       'cursor': 'pointer'}},
#                                        'iconStyle':{'position':'fixed',
#                                                     'left':'7.5px',
#                                                     'text-align': 'left'},
#                                        'tabStyle' : {'list-style-type': 'none',
#                                                      'margin-bottom': '30px',
#                                                      'padding-left': '30px'}},
#                              key="0")

# with st.sidebar:
#    st.sidebar.title('Menu')
#    st.button('Home')
#    add_selectbox = st.sidebar.selectbox(
#       "How would you like to be contacted?",
#       ('Bu', "Home phone", "Mobile phone")
#    )


with st.sidebar:
   selected2 = option_menu(None, ["Home", "My Business", 'Settings'], 
   icons=['house', 'cloud-upload', 'gear'], 
   menu_icon="cast", default_index=0, orientation="vertical",
   styles={
        "container": {"padding": "0!important", "background-color": "#109138"},
        "icon": {"color": "#F4C01E", "font-size": "25px"}, 
        "nav-link": {"font-size": "25px", "text-align": "left", "margin":"0px", "--hover-color": "#16C64D"},
        "nav-link-selected": {"background-color": "#16C64D" },
    })

## HORIZONTAL ##
# selected3 = option_menu(None, ["Home", "Upload",  "Tasks", 'Settings'], 
#    icons=['house', 'cloud-upload', "list-task", 'gear'], 
#    menu_icon="cast", default_index=0, orientation="horizontal",
#    styles={
#         "container": {"padding": "0!important", "background-color": "#fafafa"},
#         "icon": {"color": "orange", "font-size": "25px"}, 
#         "nav-link": {"font-size": "25px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
#         "nav-link-selected": {"background-color": "green"},
#     })

# st.sidebar.title('Menu')
# paginas = st.sidebar.selectbox("Select :",['pagina1','pagina2'])

##################
#### FUNTIONS ####
##################

def metricas(): 
   review_stars = business['stars'].mean()
   review['positive_score'] = review['pos_reviews'] / ( review['neg_reviews'] + review['pos_reviews'])
   Positive_sentiment = review['positive_score'].mean()
   review_total = review.shape[0]
   number_visits = checkin['number_visits'].sum()

   st.markdown("### Oportunities")
   oportunity = st.columns(3)
   oportunity[0].metric('Business Line', '98,7%', delta=None, delta_color="normal")
   oportunity[1].metric('Location', '93,5%', delta=None, delta_color="normal")
   oportunity[2].metric('Services', '90,7%', delta=None, delta_color="normal")

   st.markdown("### Account Summary")
   metrics = st.columns(6)
   metrics[0].metric('Review Total', review_total, delta=None, delta_color="normal")
   metrics[1].metric('Review stars', round(review_stars, 2), delta=None, delta_color="normal")
   metrics[2].metric('Positive sentiment', f'{round(Positive_sentiment, 2)*100}%', delta=None, delta_color="normal")
   metrics[3].metric('Influencer Score', '98,7%', delta=None, delta_color="normal")
   metrics[4].metric('Top Hour', '18:00', delta=None, delta_color="normal")
   metrics[5].metric('Number_visits', number_visits)

# 47OfnYwhB3NTM8Tx_sNnbw
# m1HVolBJiYajyq07J550jQ
# _EqGhRXzlUaTpu5eToC8MA
# ytynqOUb3hjKeJfRj5Tshw
# ynuDiKFEoESUpYf0QP-Ulw
# Xs95WXSbawqZFmJ0nhahJQ
# LObAexsCZ9mgh_xLPK2S2w
# ThztWldIIslYvTiTA_CMtg

# business_name = business['name'].to_list()
# business_name


## HOME ## 
if selected2 == "Home":
	st.subheader("Home")

metricas() 

## My Business
if selected2 == "My Business":
	st.subheader("My Business")

	with st.form(key='searchForm'):
		search_movie = st.text_input("Search Movie")
		submit_button = st.form_submit_button(label='Search')
