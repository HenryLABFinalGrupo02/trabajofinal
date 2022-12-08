import streamlit as st
import pandas as pd 
from streamlit_option_menu import option_menu
from multiprocessing import Value
from pages.main import tabs_functions as tf
from PIL import Image
import authenticator_edit as stauth
import yaml

with open(r'config.yaml') as file:
   users = yaml.load(file, Loader=yaml.FullLoader)


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

def register():
   try:
      if authenticator.register_user('Register user', preauthorization=True):
         st.success('User registered successfully')
   except Exception as e:
      st.error(e)

   with open('config.yaml', 'w') as file:
      yaml.dump(users, file, default_flow_style=False)

authenticator = stauth.Authenticate(
   users['credentials'],
   users['cookie']['name'],
   users['cookie']['key'],
   users['cookie']['expiry_days'],
   users['preauthorized']
)

name, authentication_status, username,premium = authenticator.login('Login', 'main')
if authentication_status == False:
   st.error('Username/password is incorrect')
   st.button('Register', on_click=register)
if authentication_status == None:
   st.markdown('#### Please enter your username and password')
   st.button('Register', on_click=register)
if authentication_status:



 ################
###### MENU ######
 ################

   with st.sidebar:
      st.image(Image.open('./image/logo_vocado (5).png'))
   
      selected2 = option_menu(None, ["Home", "My Business", "Competition", "Opportunities", "Add business",'Log out'], 
      icons=['house', 'building', 'globe', 'star', 'plus','lightbulb-off-fill'], 
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
   if premium:
      if selected2 == "Home":
         st.title('Welcome to Vocado Admin Center')
         tf.metricas()
         #authenticator.logout('Logout', 'main')
         
      ## My Business
      if selected2 == "My Business":
         st.title('Business Admin Center')
         tf.select_business()
      
         #tf.sentiment_review()
      
      
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
      
      if selected2 == "Add business":
         tf.addbusiness()
      if selected2 == 'Log out':
         st.title("Do you want to log out?")
         authenticator.logout('Logout', 'main')
      if selected2 == 'mapa':
         tf.mapa3dGrafico()
   else:
      if 'button' not in st.session_state:
         st.session_state.button= False 
      def funButton():
         st.session_state.button=True
      if selected2 == "Home":
         st.title('Welcome to Vocado Admin Center')
         tf.metricas()
         #authenticator.logout('Logout', 'main')
      if selected2 == "My Business":
         st.title("Try our free premium for 1 month to get to know all of Vocado's functionalities")
         if st.button("Try premium!",on_click=funButton) or st.session_state.button:
            tf.select_business()
      if selected2 == "Competition":
         st.title("Try our free premium for 1 month to get to know all of Vocado's functionalities")
         if st.button("Try premium!",on_click=funButton) or st.session_state.button:
            tf.timeseries()
      if selected2 == "Opportunities":
         st.title("Try our free premium for 1 month to get to know all of Vocado's functionalities")
         if st.button("Try premium!",on_click=funButton) or st.session_state.button:
            tf.machine_learning()
      if selected2 == "Add business":
         st.title("Try our free premium for 1 month to get to know all of Vocado's functionalities")
         if st.button("Try premium!",on_click=funButton) or st.session_state.button:
            tf.addbusiness()
      if selected2 == 'Log out':
         st.title("Do you want to log out?")
         authenticator.logout('Logout', 'main')

         
