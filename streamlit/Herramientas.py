import streamlit as st
import pandas as pd 
import numpy as np
from PIL import Image
import pydeck as pdk
import plotly.express as px

class HerramientaTrabajo:
    def mapa3d(self,df):
        st.pydeck_chart(pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=df['lat'].mean(),
                longitude=df['lon'].mean(),
                zoom=10,
                pitch=50,
            ),
            layers=[
                pdk.Layer(
                   'HexagonLayer',
                   data=df,
                   get_position='[lon, lat]',
                   radius=200,
                   elevation_scale=4,
                   elevation_range=[0, 1000],
                   pickable=True,
                   extruded=True,
                ),
                pdk.Layer(
                    'ScatterplotLayer',
                    data=df,
                    get_position='[lon, lat]',
                    get_color='[200, 30, 10, 160]',
                    get_radius=200,
                ),
            ],
            tooltip={
            'html': '<b>Elevation Value:</b> {elevationValue}',
           'style': {
             'color': 'white'
            }
           },   
        ))
    def funcionLineal(self,df,columnaX, columnaY,use_container_width=True):
       st.line_chart(data=df, x=columnaX, y=columnaY, width=700, height=500, use_container_width=use_container_width)

    def barras(self,df,columnaX, columnaY,use_container_width=True):
        st.bar_chart(data=df, x=columnaX, y=columnaY, width=700, height=500, use_container_width=use_container_width)
       
    def pie(self,df,values,names,title,top):
        df = df.head(top)
        fig = px.pie(df, values=values, names=names, title=title)
        st.write(fig)
    def metrica(self,label,value,delta=None,delta_color="normal"):
        st.metric(label, value, delta=delta, delta_color=delta_color)
    def tabla(self,tabla):
        st.table(tabla)


ht = HerramientaTrabajo()

