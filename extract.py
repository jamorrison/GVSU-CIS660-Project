import requests
import zipfile
import pandas as pd

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

# TODO: Add documentation
def csv_to_df(data):
    lines = [row.split(',') for row in data.split('\n')]

    # TODO: Add NA from blank string
    return pd.DataFrame(lines)

def get_data(zh, sub_fname):
    with zh.open(sub_fname) as zf:
        data = zf.read().decode('utf-8').strip()
        df = csv_to_df(data)

    return df

def retrieve_csv_files(fname):
    with zipfile.ZipFile(fname) as zh:
        df1 = get_data(zh, 'Teams.csv')
        df2 = get_data(zh, 'TeamSplits.csv')
        df3 = get_data(zh, 'TeamVsTeam.csv')

    return (df1, df2, df3)

def extract():
    # Remote URL and local filename
    kaggle_file = 'https://www.kaggle.com/api/v1/datasets/download/open-source-sports/professional-hockey-database'
    local_file = 'professional_hockey_database.zip'

    try:
        download_file(kaggle_file, local_file)
    except requests.HTTPError:
        print(f'Unable to download file from: {kaggle_file}')
        exit(1)

    # TODO: Need to do joins on data
    df1, df2, df3 = retrieve_csv_files(local_file)

    # TODO: Delete downloaded local file once all data has been extracted

extract()
