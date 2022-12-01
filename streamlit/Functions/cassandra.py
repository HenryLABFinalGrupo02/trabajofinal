import json
from cassandra.auth import PlainTextAuthProvider 
from cassandra.cluster import Cluster

###########################
## CONNECT TO CASSANDRA ###
###########################

cloud_config= {'secure_connect_bundle': r'secure-connect-henry.zip'}
auth_provider = PlainTextAuthProvider(json.load(open(r'log_in.json'))['log_user'], json.load(open(r'log_in.json'))['log_password'])

def connect_to_astra():
   """
   It establishes a connection to the Astra database.
   :return: A session object
   """
   print('ESTABLISHING CONNECTION TO CASSANDRA')
   cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
   session = cluster.connect()
   return session

session = connect_to_astra()

# session.execute('use yelp;')
tables = session.execute('describe yelp;')

print(tables)