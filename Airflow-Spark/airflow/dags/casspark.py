import databricks.koalas as ks
import pandas as pd
from IPython.display import display, clear_output


def cql_to_spark_pandas(cql_query,cassandra_session):
    def pandaspark_factory(colnames, rows):\
        return ps.DataFrame(rows, columns=colnames)
    cassandra_session.row_factory = pandaspark_factory
    cassandra_session.default_fetch_size = None
    result = cassandra_session.execute(cql_query, timeout=None)
    return result._current_rows


def spark_pandas_insert(dataframe,keyspace,table,cassandra_session, debug=False):

    """
    Inserts a PySpark Pandas Dataframe into a Cassandra Table

    Arguments:
    dataframe: pyspark.pandas.frame.DataFrame
    keyspace: str
    table: str
    cassandra_session: cassandra.cluster.Session
    """

    _iter = 1
    _df_len = len(dataframe)
    print('DEFINING QUERY')
    query = "INSERT INTO {keyspace_str}.{table_str}({columns_str}) VALUES ({emptycols})".format(
    keyspace_str=keyspace,
    table_str=table,
    columns_str=','.join(list(dataframe.columns)),
    emptycols=','.join(['?' for i in list(dataframe.columns)])
    )
    print('PREPARING QUERY')
    prepared = cassandra_session.prepare(query)
    print('LOOPING')
    for index, row in dataframe.iterrows():
        cassandra_session.execute(prepared,([row[column] for column in list(dataframe.columns)]))
        if debug:
            _iter+=1
            _progress = round((_iter/_df_len)*100,2)
            _progress_bar = int(_progress)
            if(_iter%10000 == 0): 
                clear_output()
                print('DATAFRAME QUERY\n[',_progress_bar*'#',(100-_progress_bar)*'.',']',_progress,'%')
            if(_iter >= _df_len-5):
                clear_output()
                print('FINISHING QUERY\n[ #################################################################################################### ] 100 %')

def table_in_keyspace(keyspace,table,cassandra_session):
    """
    Returns True if a table exists in an certain keyspace

    Arguments:
    keyspace: str
    table: str
    cassandra_session: cassandra.cluster.Session
    """

    query = """
SELECT table_name
FROM system_schema.tables WHERE keyspace_name='{keyspace_str}';
""".format(keyspace_str=keyspace)
    exists = table in cql_to_spark_pandas(query,cassandra_session).values
    return exists