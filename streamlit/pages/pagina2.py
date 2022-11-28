import streamlit as st
import pandas as pd 
import numpy as np
from PIL import Image
import pydeck as pdk
from Herrsmientas import ht

a = st.write("# Welcome to Streamlit! 👋")

dicc = {'cantidad':[12,88,90,70],'pais':['Argentina','Colombia','Brasil','Peru']}
df = pd.DataFrame(dicc)
#df, values=values, names=names, title=title

topN = st.text_input('Top N:',3)
topN = int(topN)

ht.pie(df,'cantidad','pais','prueba',topN)
ht.metrica("Gas price",4,-0.5,"inverse")