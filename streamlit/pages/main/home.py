import streamlit as st
import pandas as pd 
from streamlit_option_menu import option_menu
from cgitb import text
from multiprocessing import Value
from os import write
from turtle import onclick, onscreenclick
from typing import List
from numpy.core.fromnumeric import size
from PIL import Image
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from pathlib import Path


def cql_to_pandas(cql_query,cassandra_session):
    def pandaspark_factory(colnames, rows):\
        return pd.DataFrame(rows, columns=colnames)
    cassandra_session.row_factory = pandaspark_factory
    cassandra_session.default_fetch_size = None
    result = cassandra_session.execute(cql_query, timeout=None)
    return result._current_rows


cloud_config= {'secure_connect_bundle': r'secure-connect-henry.zip'}
auth_provider = PlainTextAuthProvider(json.load(open(r'log_in.json'))['log_user'], json.load(open(r'log_in.json'))['log_password'])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()



checkin = pd.read_csv(r'data/checkin_1000.csv')
review = pd.read_csv(r'data/review_1000.csv')
im = Image.open('pages\main\logo.png')



def metricas(business_id):
   business = cql_to_pandas("""SELECT * FROM yelp.business WHERE business_id = '{}' ALLOW FILTERING; """.format(business_id),session)
   #review = cql_to_pandas("""SELECT * FROM yelp.review WHERE business_id = '{}' ALLOW FILTERING; """.format(business_id),session)
   review_stars = business['stars']
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
   metrics[1].image(im,width=50)
   metrics[1].metric('Review stars', round(review_stars, 2), delta=None, delta_color="normal")
   metrics[2].metric('Positive sentiment', f'{round(Positive_sentiment, 2)*100}%', delta=None, delta_color="normal")
   metrics[3].metric('Influencer Score', '98,7%', delta=None, delta_color="normal")
   metrics[4].metric('Top Hour', '18:00', delta=None, delta_color="normal")
   metrics[5].metric('Number_visits', number_visits)



   