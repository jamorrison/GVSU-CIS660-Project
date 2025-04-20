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
    # Number of teams in NHL by year
    teams_per_year = df[['tmID', 'year']].drop_duplicates().groupby(['year']).count()
    teams_per_year.columns = ['n_teams']
    print('\n\nNumber of teams in the NHL by year')
    print(teams_per_year)

    # Mean winning percentage (WP_split)
    avg_winning_percentage = df[['tmID', 'WP_split']].drop_duplicates()
    print('\n\nAverage winning percentage across all teams and years')
    print('\t{}'.format(round(avg_winning_percentage['WP_split'].mean(), 3)))

    # Mean home winning percentage (WP_home)
    avg_home_winning_percentage = df[['tmID', 'WP_home']].drop_duplicates()
    print('\n\nAverage home winning percentage across all teams and years')
    print('\t{}'.format(round(avg_home_winning_percentage['WP_home'].mean(), 3)))

    # Mean road winning percentage (WP_road)
    avg_road_winning_percentage = df[['tmID', 'WP_road']].drop_duplicates()
    print('\n\nAverage road winning percentage across all teams and years')
    print('\t{}'.format(round(avg_road_winning_percentage['WP_road'].mean(), 3)))

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
