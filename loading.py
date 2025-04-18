import logging
import sqlite3
import pandas as pd

logger = logging.getLogger('load')

DB_NAME = 'data_warehouse.sqlite'
TB_NAME = 'hockey_teams'

def db_connection(verbose=True):
    """Establish connection to database. Will implicitly create it if it doesn't exist

    Inputs -
        None
    Returns -
        sqlite3.Connection
    """
    con = sqlite3.connect(DB_NAME)
    if verbose:
        logger.info(f'Successfully established connection to: {DB_NAME}')

    return con

def validate(n_expected):
    """Validate load occurred successfully

    Inputs -
        n_expected - number of lines expected to be written (int)
    Returns -
        bool
    """
    con = db_connection(verbose=False)

    # Count number of lines in written table
    query = f'SELECT COUNT(*) AS n_lines FROM {TB_NAME};'
    result = pd.read_sql(query, con=con)

    return n_expected == result['n_lines'][0]

def load(df):
    """Load transformed data into SQLite database.

    Inputs -
        df - DataFrame from transform.transform()
    Returns -
        None
    """
    con = db_connection()

    # Write transformed data to SQLite database
    n_written = df.to_sql(
        name = TB_NAME,
        con = con,
        if_exists = 'replace',
        dtype = {
            'year'     : 'int',
            'lgID'     : 'str',
            'tmID'     : 'str',
            'oppID'    : 'str',
            'W_TvT'    : 'int',
            'L_TvT'    : 'int',
            'T_TvT'    : 'int',
            'OTL_TvT'  : 'int',
            'franchID' : 'str',
            'confID'   : 'str',
            'divID'    : 'str',
            'rank'     : 'str',
            'G'        : 'int',
            'W_split'  : 'int',
            'L_split'  : 'int',
            'T_split'  : 'int',
            'OTL_split': 'int',
            'Pts'      : 'int',
            'SoW'      : 'int',
            'SoL'      : 'int',
            'GF'       : 'int',
            'GA'       : 'int',
            'name'     : 'str',
            'hW'       : 'int',
            'hL'       : 'int',
            'hT'       : 'int',
            'hOTL'     : 'int',
            'rW'       : 'int',
            'rL'       : 'int',
            'rT'       : 'int',
            'rOTL'     : 'int',
            'WP_TvT'   : 'float',
            'WP_split' : 'float',
            'WP_home'  : 'float',
            'WP_road'  : 'float',
        },
    )

    logger.info(f'{n_written} lines written to {TB_NAME} table in {DB_NAME}')

    if validate(len(df)):
        logger.info('Successfully loaded all data into database')
    else:
        logger.error('Error loading data into database!')
        exit(1)

    return None
