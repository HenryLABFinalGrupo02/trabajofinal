#import conexion
import streamlit as st
import pandas as pd 
from multiprocessing import Value
from typing import List

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

def select_business(): 
    """
    The function reads in a csv file with a list of business names, and then creates a multi-select
    widget that allows the user to select multiple business names from the list. 
    """
    business = pd.read_csv(r'data/business_1000.csv')
    name_business = business['name']

    option = st.multiselect(
        'My businesses',
        (name_business.to_list()))

    st.write('You selected:', option)

def selete_business(): 
    business_name = cql_to_pandas("""select name from yelp.business ALLOW FILTERING;""",conexion.seccion)
    option = st.selectbox(
        'My businesses',
        (business_name.to_list()))
    st.write('You selected:', option)