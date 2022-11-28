import streamlit as st
import pandas as pd 
import numpy as np
import pydeck as pdk
from Herrsmientas import ht



df  = pd.DataFrame(
    np.random.randn(20, 2),
    columns=['a', 'b'])

#st.line_chart(data=df, x=df['a'], y=df['b'], use_container_width=True)
#funcionLineal(chart_data,'a','b',False)
ht.funcionLineal(df,'a','b',True)
#st.line_chart(df,x='a',y='b',use_container_width=True)
ht.barras(df,'a','b')
