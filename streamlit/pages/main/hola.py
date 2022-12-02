import sqlalchemy
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@{address}/{db}".format(user="root",
            address = '35.239.80.227:3306',
            pw="Henry12.BORIS99",
            db="yelp"))

print(engine.table_names())