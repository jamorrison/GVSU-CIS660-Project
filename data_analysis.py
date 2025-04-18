import logging
import sqlite3
import pandas as pd

from loading import db_connection, TB_NAME

logger = logging.getLogger('data analysis')

def analysis(df):
    """Calculate and print some basic statistics from data

    Inputs -
        df - DataFrame
    Returns -
        None
    """
    # TODO: Number of teams in NHL by year

    # TODO: Mean winning percentage (WP_split)

    # TODO: Mean home winning percentage (WP_home)

    # TODO: Mean road winning percentage (WP_road)

    return None

def visualize(df):
    """Visualize data

    Inputs -
        df - DataFrame
    Returns -
        None
    """
    # TODO: Visualization 1 - goals scored vs goals against (colored by record (>0.500, 0.500, <0.500))

    # TODO: Visualization 2 - mean goals scored by year per team

    return None

def do_analysis():
    """Perform data analysis and visualization together

    Inputs -
        None
    Returns -
        None
    """
    con = db_connection()

    # Load data from database
    query = f'SELECT * FROM {TB_NAME};'
    df = pd.read_sql(query, con=con, index_col='index')

    # Data analysis
    analysis(df)

    # Data visualization
    visualize(df)

    return None
