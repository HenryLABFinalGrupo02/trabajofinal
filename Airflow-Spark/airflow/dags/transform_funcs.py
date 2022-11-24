####################################
######## LIBRARIES IMPORT ##########
####################################


import databricks.koalas as ks
import pyspark.pandas as ps
import pandas as pd
from pathlib import Path
from IPython.display import display, clear_output
import dill

####################################
######## PATH SETTINGS ##########
####################################


# HELPER FUNCTIONS

def values_type(dataframe, column): 
    """
    This functions returns a set that contains the data types contained within a column of a pandas or koalas dataframe.

    Parameters:
    - dataframe: pandas or koalas dataframe
    - column: the name of the column to analize
    """
    types = set()
    for value in dataframe[column].to_numpy():
        types.add(type(value))
    return types

# DEALING WITH DUPLICATED REGISTERS

def drop_duplicates(Table):
    """
    Returns a Dataframe with no duplicates

    Parameters:
    - Table: Pandas or Koalas dataframe
    """
    return Table.drop_duplicates()


####################################
######## COMMON FUNCTIONS ##########
####################################
def import_json(file:str, path, format:str = 'json'):
    '''
    This function imports files with spark and transforms them into DataFrame using the koala library

    Arguments:
    :: file: str of the file name
    :: path: 'path' path where the file is stored
    :: format: 'str' file format 

    Returns: 
    ---------
    Dataframe and print shape 
    '''
    path_final = path + file
    print('READING JSON')
    df = ks.read_json(path_final, lines=True)
    print(f"Shape of {file} is {df.shape}")
    return df
    

def upload_to_cassandra(df, table_name):
    df.write.format("org.apache.spark.sql.cassandra")\
    .options(table=table_name, keyspace="yelp")\
    .mode('append')\
    .save()


# ID VALIDATION

def check_id_chars(Table, id_column):
    """
    Checks if the strings in an ID column have the required characters (20).
    This function is meant to be called within the 'drop_bad_ids' function.
    Returns a list of indexes at which the column has an invalid ID.

    Parameters:
    - Table: Pandas or Koalas dataframe
    - id_column: column containing 22 character ID's
    """
    problems = []
    for index, value in Table[id_column].items() :
        if len(value) != 22:
            problems.append(index)
    return problems

def drop_bad_ids(Table, id_column):
    """
    This function removes the rows in a table where an ID is not valid.
    Returns a table with only valid ID's in the passed column.

    Parameters:
    - Table: Koalas dataframe
    - id_column: column containing 22 character ID's
    """
    id_list = check_id_chars(Table, id_column)
    return Table[ks.Series((~Table.index.isin(id_list)).to_list())].reset_index(drop=True)

# NUMERIC VALUES

def impute_num(Table, col_list, absolute=False):
    """
    This function replaces missing values in numeric columns with 0.
    If the 'absolute' parameter is passed, the function also converts 
    the numeric columns into their absolute value.

    Parameters:
    - Table: Pandas or Koalas dataframe
    - col_list: list of numeric columns with missing values to be imputed
    - absolute: boolean, decides if the column will contain absolute values. Default: False.
    """
    for col in col_list:
        Table[col].fillna(0)
        if absolute:
            Table[col] = Table[col].apply(lambda x: abs(x))

# STRING VALUES

def clean_string(string):
    """
    This function cleans strings by removing whitespaces at the beginning and 
    at the end of the string, replacing double spaces with single spaces and
    converting the string to lower case.
    It is meant to be used within the 'drop_bad_str' function.
    Returns a clean string.

    Parameters:
    - string: some string to be cleaned
    """
    new_str = string.strip().replace('  ',' ').lower()
    return new_str

def drop_bad_str(Table, col):
    """
    This function takes a Dataframe and the name of a column that contains string values, 
    imputes missing values in the column, cleans it's strings and removes registers where 
    the string in the column has 2 or less characters.
    The function returns the dataframe after performing the above mentioned transformations
    and dropping the unwanted registers.

    Parameters:
    - Table: Pandas or Koalas dataframe
    - col: string, the name of the column to transform
    """
    T_ok = Table.copy()
    T_ok[col] = T_ok[col].fillna('NO DATA')
    T_ok[col] = T_ok[col].apply(clean_string)
    bad_strs = []
    for index, tip in T_ok[col].items():
        if len(tip) <=2:
            bad_strs.append(index)
    return T_ok[ks.Series((~Table.index.isin(bad_strs)).to_list())].reset_index(drop=True)


# DATETIME VALUES

def transform_dates(dataframe,column,format):
    """
    This function recieves 1) a dataframe, 2) the name of a column containing timestamp values
    and 3) a date format. It returns the dataframe after transforming the column to the desired 
    format.
    
    Parameters:
    - dataframe: a Koalas dataframe
    - column: the name of the column containing timestamp values
    - format: the datetime format to which the column will be transformed
    """
    series = ks.to_datetime(dataframe[column], errors='coerce')
    mode = series.mode().iloc[0].strftime(format)
    series = series.apply(lambda x: mode if (x is pd.NaT) else x.strftime(format))
    return series

# LISTS OF STRINGS

def check_str_list(ls):
    """
    This function recieves a list and returns a second list containing only the strings from the
    original list. In case there were none, it returns an empty list. If a None value is passed, 
    the function returns an empty list.
    """
    try:
        ls_ok = []
        for x in ls:
            if type(x) == str:
                ls_ok.append(x)
        return ls_ok
    except:
        return []

# DICTIONARY

def row_hours_to_list(row):
    """
    Returns a list of lists, each sublist containing the day of the week, it's opening hour and it's closing hour. E.g.: [[1,8,18],[2,8,18]...]

    Parameters:
    - row: pyspark row object
    """
    dicc = row.asDict()
    day_dicc = {
        'Monday': 1,
        'Tuesday': 2,
        'Wednesday': 3,
        'Thursday': 4,
        'Friday': 5,
        'Saturday': 6,
        'Sunday': 7
    }

    check = zip(dicc.keys(),list(map(lambda x: x.split('-') if isinstance(x,str) else x,dicc.values())))
    
    return [[day_dicc[key],
            int(value[0].split(':')[0])+int(value[0].split(':')[1]),
            int(value[1].split(':')[0])+int(value[1].split(':')[1])
            ] if value is not None else [day_dicc[key],0,0] for key,value in check]

def row_hours_to_series(series):
    """
    This function takes a column from a koalas dataframe that contains a dictionary with each day of the week as a key and
    the opening and closing schedules for the day as the value.
    The function returns a koalas series whose elements are lists of lists in the same format as the outputed by the 
    'row_hours_to_list' function.

    Parameters:
    - series: koalas series
    """
    series_mode = row_hours_to_list(series.mode().iloc[0])
    series_output = []
    for index, value in series.items():
        if value is None:
            series_output.append(series_mode)
        else:
            series_output.append(row_hours_to_list(value))
    return ks.Series(series_output)


def get_date_as_list(value):
    ls = value.split(', ')
    return ls

def get_total_checkins(value):
    ls = value.split(', ')
    return len(ls)

def get_state_city(df):
    print('SETTING OPTION')
    ks.set_option('compute.ops_on_diff_frames', True)
    print('GeTTING CITY LIST')
    cities = list(df.city.to_numpy())
    print('GeTTING STATE LIST')
    states = list(df.state.to_numpy())
    print('GET LIST')
    st_ct_list = [[states[i],cities[i]] for i in range(len(cities))]
    print('OBTAINING SERIES')
    dill.extend(False)
    state_city = ks.Series(st_ct_list)
    dill.extend(True)
    print('CREATING COLUMN')
    df['state_city'] = state_city