import streamlit as st
import pandas as pd 
from multiprocessing import Value
from typing import List
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from pathlib import Path
import os

def cql_to_pandas(cql_query,cassandra_session):
    """
    It takes a CQL query and a Cassandra session as input, and returns a Pandas dataframe
    
    :param cql_query: The CQL query you want to run
    :param cassandra_session: The Cassandra session object
    :return: A pandas dataframe
    """
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


business = cql_to_pandas("""select * from yelp.business ALLOW FILTERING;""",session)
#checkin = cql_to_pandas("""select * from yelp.checkin ALLOW FILTERING;""",session)
#review = cql_to_pandas("""select * from yelp.review ALLOW FILTERING;""",session)
review = pd.read_csv(r'pages/main/data/review_1000.csv')
checkin = pd.read_csv(r'pages/main/data/checkin_1000.csv')
print(os.listdir)


def l(filtro):
   ids = filtro['business_id'].to_list()
   review_stars = filtro['stars'].mean()
   review1 = review.loc[review['business_id'].isin(ids)]
   checkin1 = checkin.loc[checkin['business_id'].isin(ids)]
   
   review1['positive_score'] = review1['pos_reviews'] / ( review1['neg_reviews'] + review1['pos_reviews'])
   Positive_sentiment = review1['positive_score'].mean()
   review_total = review1.shape[0]
   number_visits = checkin1['number_visits'].sum()   
   st.markdown("### Account Summary")
   
   metrics = st.columns(6)
   
   metrics[0].metric('Review Total',review_total, delta=None, delta_color="normal")
   metrics[1].metric('Review stars', round(review_stars, 2), delta=None, delta_color="normal")
   metrics[2].metric('Positive sentiment', f'{round(Positive_sentiment, 2)*100}%', delta=None, delta_color="normal")
   metrics[3].metric('Influencer Score', '98,7%', delta=None, delta_color="normal")
   metrics[4].metric('Top Hour', '18:00', delta=None, delta_color="normal")
   metrics[5].metric('Number_visits', number_visits)
        
def select_business(): 
    name_business = cql_to_pandas("""select name from yelp.business ALLOW FILTERING;""",session)
    option = st.selectbox(
        'My businesses',
        (name_business))

    st.write('You selected:', option)
    if option in option:
        filtro = business[business['name'] == option]
        l(filtro)

