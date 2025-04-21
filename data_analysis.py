import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
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

    print('\nNumber of teams in the NHL by year')
    print('| year: N  | year: N  | year: N  | year: N  | year: N  |')
    print('|----------|----------|----------|----------|----------|', end='\n| ')
    count = 1
    for idx, row in teams_per_year.iterrows():
        end = ' |\n| ' if count % 5 == 0 else ' | '
        print(f'{idx}: {row["n_teams"]}', end=end)
        count += 1

    # Mean winning percentage (WP_split)
    avg_winning_percentage = df[['tmID', 'WP_split']].drop_duplicates()
    print(
        '\nAverage winning percentage across all teams and years: {}'.format(
            round(avg_winning_percentage['WP_split'].mean(), 3)
        )
    )

    # Mean home winning percentage (WP_home)
    avg_home_winning_percentage = df[['tmID', 'WP_home']].drop_duplicates()
    print(
        '\nAverage home winning percentage across all teams and years: {}'.format(
            round(avg_home_winning_percentage['WP_home'].mean(), 3)
        )
    )

    # Mean road winning percentage (WP_road)
    avg_road_winning_percentage = df[['tmID', 'WP_road']].drop_duplicates()
    print(
        '\nAverage road winning percentage across all teams and years: {}'.format(
            round(avg_road_winning_percentage['WP_road'].mean(), 3)
        )
    )

    return None

def visualize(df):
    """Visualize data

    Inputs -
        df - DataFrame
    Returns -
        None
    """
    # Set up DataFrames for visualizations first
    # Visualization 1 - goals scored vs goals against (colored by record (>=0.500, <0.500))
    vis1 = df[['tmID', 'GF', 'GA', 'WP_split']].drop_duplicates(j)
    vis1['color'] = ['red' if x >= 0.5 else 'black' for x in vis1['WP_split']]

    # Visualization 2 - mean goals scored by year per team
    by_team = df[['tmID', 'name', 'year', 'GF']].drop_duplicates()

    # Set up figure
    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(10,5))

    # Visualization 1
    ax1.plot([100, 450], [100, 450], 'b-')
    ax1.scatter(vis1['GF'], vis1['GA'], c=vis1['color'])

    col_legend = [Line2D([0], [0], color='red'), Line2D([0], [0], color='black')]
    ax1.legend(col_legend, ['At least 0.500', 'Below 0.500'], title='Winning Percentage')

    ax1.set_title('Winning Percentage Based on\nGoals Scored For and Against')
    ax1.set_xlabel('Goals For', fontsize='large')
    ax1.set_ylabel('Goals Against', fontsize='large')

    # Visualization 2
    for team in by_team['name'].unique():
        tmp = by_team[by_team['name'] == team]
        ax2.plot(tmp['year'], tmp['GF'], '-', label=team)

    ax2.legend(ncol=2, loc='upper left', bbox_to_anchor=(1.01, 1.05), title='Teams')

    ax2.set_title('Goals Scored by Year for Each Team')
    ax2.set_xlabel('Year', fontsize='large')
    ax2.set_ylabel('Goals For', fontsize='large')

    fig.savefig('data_visualizations.pdf', bbox_inches='tight')

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
