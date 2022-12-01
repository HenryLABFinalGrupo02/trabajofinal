import streamlit as st
import pandas as pd 
import numpy as np
from multiprocessing import Value
from typing import List
#import darts 
import plotly as py
pd.options.plotting.backend = 'plotly'
from darts import TimeSeries
from darts.models import ExponentialSmoothing
from darts.metrics import mape
import plotly.express as px
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.graph_objects as go


def eval_model(model, train, val):
    model.fit(train)
    forecast = model.predict(len(val))
    
    string = "model {} obtains MAPE: {:.2f}%".format(model, mape(val, forecast))

    fig1 = px.line(train.pd_dataframe())
    fig1.update_layout(title='Actual')
    fig1.update_traces(line_color='purple', name='Actual')


    fig2 = px.line(forecast.pd_dataframe())
    fig2.update_layout(title='Forecast')
    fig2.update_traces(line_color='seagreen', name='Forecast')

    fig = go.Figure(data = fig1.data + fig2.data)
    return fig, string

def timeseries():

    df = pd.read_csv('./data/forecasting.csv', parse_dates=['month'], index_col='month')
    df = df['2010':]

    # st.title('Time Series Visualization')
    st.markdown('Reviews/Tips/Checkins by Month for the Top Brands in USA'
    )

    # Create a list of unique brands
    st.text("Select you favourite brand")
    top_brand_selected = st.selectbox('Select brand', df.columns.tolist())

    st.plotly_chart(df[top_brand_selected].plot(title = 'Total Review/Tips/Checkins Counts on Yelp for Top Brands'))

    series = TimeSeries.from_dataframe(df, fill_missing_dates=True, freq='MS', fillna_value=0)

    st.title('Forecasting Time Series')
    st.markdown('Reviews/Tips/Checkins by Month for the Top Brands in USA'
    )

    # Create a list of unique brands
    st.text("Select you favourite brand")
    top_brand_selected_f = st.selectbox('Select brand for forecast', df.columns.tolist())

    train, val = series[top_brand_selected_f].split_after(pd.Timestamp('2021-01-01'))
    
    model = ExponentialSmoothing()

    fig, string = eval_model(model, train, val)

    fig.update_layout(title=top_brand_selected_f)
    st.plotly_chart(fig)

    st.text(string)



# def forecasting():

#     df = pd.read_csv('./data/forecasting.csv', parse_dates=['month'], index_col='month')
#     df = df['2010':]

    

