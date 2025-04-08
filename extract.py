import requests
import zipfile
import pandas as pd
import numpy as np
import os

# Code is based on this StackOverflow post:
#     https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
def download_file(url, fname):
    """Download file from a provided URL

    Inputs -
        url   - URL to file to download
        fname - name of local file that remote file is saved to
    Returns -
        None
    Raises -
        requests.HTTPError
    """
    # Open up HTTP request
    with requests.get(url, stream=True) as r:
        # Ensure successful response
        r.raise_for_status()

        # Open file to write retrieved data
        with open(fname, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return None

def csv_to_df(data):
    """Turn a CSV file into a DataFrame.

    Inputs -
        data - file contents as a string
    Returns -
        pandas.DataFrame
    """
    lines = [row.split(',') for row in data.split('\n')]

    tmp = pd.DataFrame(lines)
    tmp.columns = tmp.iloc[0]
    tmp = tmp.drop(tmp.index[0])

    # This returns a FutureWarning that should be addressed, but will be ignored for now
    return tmp.replace(r'^\s*$', np.nan, regex=True)

def get_data(zh, sub_fname):
    """Retrieve data from within a zip file

    Inputs -
        zh        - zip file handle
        sub_fname - compressed file name within zip file
    Returns -
        pandas.DataFrame
    """
    with zh.open(sub_fname) as zf:
        data = zf.read().decode('utf-8').strip()
        df = csv_to_df(data)

    return df

def retrieve_csv_files(fname):
    """Get the specific data that I want to parse

    Inputs -
        fname - file name of zip file
    Returns -
        tuple, (pandas.DataFrame, pandas.DataFrame, pandas.DataFrame)
    """
    with zipfile.ZipFile(fname) as zh:
        df1 = get_data(zh, 'Teams.csv')
        df2 = get_data(zh, 'TeamSplits.csv')
        df3 = get_data(zh, 'TeamVsTeam.csv')

    return (df1, df2, df3)

def extract():
    """Pull data from Kaggle and extract to joined DataFrame

    Inputs -
        None
    Returns -
        pandas.DataFrame
    """
    # Remote URL and local filename
    kaggle_file = 'https://www.kaggle.com/api/v1/datasets/download/open-source-sports/professional-hockey-database'
    local_file = 'professional_hockey_database.zip'

    # Download data
    try:
        download_file(kaggle_file, local_file)
    except requests.HTTPError:
        print(f'Unable to download file from: {kaggle_file}')
        exit(1)

    # Extract data
    df1, df2, df3 = retrieve_csv_files(local_file)

    # Merge data
    df = pd.merge(df1, df2, on=['year', 'lgID', 'tmID'], how='left')
    df = pd.merge(df3, df, on=['year', 'lgID', 'tmID'], how='left', suffixes=['_TvT', '_split'])

    try:
        os.remove(local_file)
    except FileNotFoundError:
        print('Attempted to delete {local_file}, but could not find it')

    return df

if __name__ == '__main__':
    df = extract()
    print(df)
