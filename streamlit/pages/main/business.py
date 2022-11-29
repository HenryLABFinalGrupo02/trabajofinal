import streamlit as st
import pandas as pd 
from streamlit_option_menu import option_menu
from cgitb import text
from multiprocessing import Value
from os import write
from turtle import onclick, onscreenclick
from typing import List
from numpy.core.fromnumeric import size



def func():
    with st.form(key='searchForm'):
        search_movie = st.text_input("Search Movie")
        ubmit_button = st.form_submit_button(label='Search')