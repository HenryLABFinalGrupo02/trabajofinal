# import libraries
import requests
import pandas as pd

def trim_all_columns(df):
    """
    > If the value is a string, strip whitespace from the ends of the string. Otherwise, return the value
    
    Arguments: 
    --------------------------------   
    :param df: The dataframe you want to trim

    Return
    --------------------------------
    A dataframe with all the values trimmed.
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)

def normalize_column(df, column_name):
    """
    > This function takes a dataframe and a column name as input, and returns a new dataframe with the
    column normalized

    Arguments: 
    --------------------------------
    :param df: the dataframe
    :param column_name: The name of the column you want to normalize
    """
    df[column_name] = df[column_name].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df[column_name]

# Source: https://stackoverflow.com/questions/9662346/python-code-to-remove-html-tags-from-a-string
def clean_values(series, to_replace, value = '', regex = True):
    """
    > This function takes a pandas series, a list of values to replace, and a value to replace them with,
    and returns a series with the values replaced.

        Arguments: 
    --------------------------------
    :param series: the series you want to clean
    :param to_replace: The value or list of values to be replaced
    :param value: the value to replace the to_replace values with
    :param regex: If True, assumes the to_replace pattern is a regular expression, defaults to True
    (optional)
    """
    for i in to_replace:
        series = series.str.replace(i, value, regex=regex)
    return series

def get_lat_lon(address, access_key = '2e843c7ee44a8f52742a8168d0121a0a', URL = "http://api.positionstack.com/v1/forward"):
    """
    > It takes an address and returns the latitude and longitude of that address

     Arguments: 
    --------------------------------   
    :param address: The address you want to get the latitude and longitude for
    :param access_key: This is the access key that you get from the website, defaults to
    2e843c7ee44a8f52742a8168d0121a0a (optional)
    :param URL: The URL of the API endpoint, defaults to http://api.positionstack.com/v1/forward
    (optional)

    Return
    --------------------------------
    A tuple of latitude and longitude
    """
    PARAMS = {'access_key': access_key, 'query': address}
    r = requests.get(url = URL, params = PARAMS)
    data = r.json()
    return data['data'][0]['latitude'], data['data'][0]['longitude']

def run_exps(X_train: pd.DataFrame , y_train: pd.DataFrame, X_test: pd.DataFrame, y_test: pd.DataFrame) -> pd.DataFrame:
    """
    > This function takes in training and test data, and then runs a bunch of models on it, returning a
    dataframe of the results
    
    Arguments: 
    --------------------------------
    :param X_train: training split 
    :param y_train: training target vector
    :param X_test: test split
    :param y_test: test target vector

    Types: 
    --------------------------------
    :type X_train: pd.DataFrame
    :type y_train: pd.DataFrame
    :type X_test: pd.DataFrame
    :type y_test: pd.DataFrame

    Return
    --------------------------------
    A dataframe of predictions
    """
    
    dfs = []

    dt = DecisionTreeClassifier(max_depth=1)

    models = [
        ('LogReg', LogisticRegression()), 
        ('RF', RandomForestClassifier()),
        ('KNN', KNeighborsClassifier()),
        ('GNB', GaussianNB()),
        ('XGB', XGBClassifier()),
        ('ADA', AdaBoostClassifier(base_estimator=dt))
        ]
    results = []
    names = []

    scoring = ['accuracy', 'precision_weighted', 'recall_weighted', 'f1_weighted', 'roc_auc']

    target_names = ['malignant', 'benign']

    for name, model in models:
            kfold = model_selection.KFold(n_splits=5, shuffle=True, random_state=90210)
            cv_results = model_selection.cross_validate(model, X_train, y_train, cv=kfold, scoring=scoring)
            clf = model.fit(X_train, y_train)
            y_pred = clf.predict(X_test)
            print(name)
            print(classification_report(y_test, y_pred, target_names=target_names))
            
    results.append(cv_results)
    names.append(name)
    this_df = pd.DataFrame(cv_results)
    this_df['model'] = name
    dfs.append(this_df)
    final = pd.concat(dfs, ignore_index=True)
    return final
