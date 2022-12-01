from pickle import dump as pk_dump
from pickle import load as pk_load
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from pathlib import Path
import pandas as pd


cloud_config= {'secure_connect_bundle': r'cached_data\secure-connect-henry.zip'}
auth_provider = PlainTextAuthProvider(json.load(open(r'cached_data\log_in.json'))['log_user'], json.load(open(r'cached_data\log_in.json'))['log_password'])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

reload_time = datetime.now().minute%5 == 0

def cql_to_pandas(cql_query,cassandra_session):
    def pandaspark_factory(colnames, rows):\
        return pd.DataFrame(rows, columns=colnames)
    cassandra_session.row_factory = pandaspark_factory
    cassandra_session.default_fetch_size = None
    result = cassandra_session.execute(cql_query, timeout=None)
    return result._current_rows

def unique_business_id():
    if reload_time:
        unique = pd.concat([pd.Series(['tYCok-NtWvg8_k7woeB83w','Wr2k0Vz8RbcumYulp-jIrA','QKFojAIRYfQQzwssuoKjzw']), cql_to_pandas("""SELECT DISTINCT business_id FROM yelp.business""",session)['business_id']])
        pk_dump(unique, open(r'cached_data\unique_business_id.pkl',"wb"))
        return unique
    else: return pk_load(open(r'cached_data\unique_business_id.pkl',"rb"))