import cassandra
from cassandra.auth import PlainTextAuthProvider 
from cassandra.cluster import Cluster
import json
import pandas as pd

cloud_config= {'secure_connect_bundle': r'secure-connect-henry.zip'}
auth_provider = PlainTextAuthProvider(json.load(open('log_in.json'))['log_user'], json.load(open('log_in.json'))['log_password'])

def connect_to_astra():
   print('ESTABLISHING CONNECTION TO CASSANDRA')
   cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
   session = cluster.connect()
   return session

seccion = connect_to_astra()

#seccion.execute('use yelp;')
#a = 'tYCok-NtWvg8_k7woeB83w'

review = seccion.execute("""select * from yelp.review where business_id='{}' ALLOW FILTERING;""".format(a))
reviewdf = pd.DataFrame(review)
#print(reviewdf)
business = seccion.execute('select * from yelp.business;')
businessdf = pd.DataFrame(business)
#
checkin = seccion.execute('select * from yelp.checkin;')
checkindf = pd.DataFrame(checkin)
#print(checkindf)
