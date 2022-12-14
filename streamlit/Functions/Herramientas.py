import streamlit as st
import pandas as pd 
import numpy as np
import pydeck as pdk
import plotly.express as px



class HerramientaTrabajo:
    def mapa3d(self,df): #This method enters a DataFrame as a parameter and returns a 3d map
        st.pydeck_chart(pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=38.78,
                longitude=-90.51,
                zoom=3,
                pitch=50,
            ),
            layers=[
                pdk.Layer(
                    'ScatterplotLayer',
                    df,
                    pickable=True,
                    opacity=0.8,
                    stroked=False,
                    filled=True,
                    radius_scale=10,
                    radiusMinPixels=30,
                    radiusMaxPixels=80,
                    get_position='[longitude_x,latitude_x]',
                    get_fill_color=[152,251,152],
                    get_line_color=[0, 0, 0],
                    
                ),],
            tooltip={"text": "{name}\n{address}\n{success_score}"} ))

    def funcionLineal(self,df,columnaX, columnaY,use_container_width=True): #this method enters a dataframe X axis and Y axis as a parameter and returns a line chart
       st.line_chart(data=df, x=columnaX, y=columnaY, width=700, height=500, use_container_width=use_container_width)

    def barras(self,df,columnaX, columnaY,use_container_width=True):  #this method enters a dataframe X axis and Y axis as a parameter and returns a bar chart
        st.bar_chart(data=df, x=columnaX, y=columnaY, width=700, height=500, use_container_width=use_container_width)
       
    def pie(self,df,values,names,title,top):#This method receives a DataFrame, values, a name, title, and a top. Returns a  chart
        df = df.head(top)
        fig = px.pie(df, values=values, names=names, title=title)
        st.write(fig)
    def metrica(self,label,value,delta=None,delta_color="normal"): #This method receives a label,value,delta,colour. Returns a metric
        st.metric(label, value, delta=delta, delta_color=delta_color)
    def tabla(self,tabla): #This method receives data and returns a table
        st.table(tabla)


ht = HerramientaTrabajo() #We instantiate the class so that we can use its methods in another file