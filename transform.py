import logging
import pandas as pd

logger = logging.getLogger('transform')

def drop_unwanted_cols(df):
    """Remove unwanted columns from input DataFrame.

    Inputs -
        df - DataFrame
    Returns -
        DataFrame
    Raises -
        KeyError
    """
    return df.drop(
        columns=[
            'PIM' , 'BenchMinor',   'PPG',   'PPC',   'SHA',
            'PKG' ,        'PKC',   'SHF',  'SepW',  'SepL',
            'SepT',      'SepOL',  'OctW',  'OctL',  'OctT',
            'OctOL',       'NovW',  'NovL',  'NovT', 'NovOL',
            'DecW',       'DecL',  'DecT', 'DecOL',  'JanW',
            'JanL',       'JanT', 'JanOL',  'FebW',  'FebL',
            'FebT',      'FebOL',  'MarW',  'MarL',  'MarT',
            'MarOL',       'AprW',  'AprL',  'AprT', 'AprOL',
            'playoff',
        ]
    )

def replace_missing_data(df):
    """Replace missing data as with context specific values.

    Inputs -
        df - DataFrame
    Returns -
        DataFrame
    """
    tmp = df.copy(deep=True)

    # Numeric values where missing data occurs means the data was either unknown or the specific value didn't
    # exist at the time. Replace with -1 instead of 0 to differentiate between unknown value and a zero value
    tmp['T_TvT']     = tmp['T_TvT'].fillna('-1')
    tmp['OTL_TvT']   = tmp['OTL_TvT'].fillna('-1')
    tmp['T_split']   = tmp['T_split'].fillna('-1')
    tmp['OTL_split'] = tmp['OTL_split'].fillna('-1')
    tmp['SoW']       = tmp['SoW'].fillna('-1')
    tmp['SoL']       = tmp['SoL'].fillna('-1')
    tmp['hT']        = tmp['hT'].fillna('-1')
    tmp['hOTL']      = tmp['hOTL'].fillna('-1')
    tmp['rT']        = tmp['rT'].fillna('-1')
    tmp['rOTL']      = tmp['rOTL'].fillna('-1')

    return tmp

def specify_column_types(df):
    """Specify numeric column types from generic object to int

    Inputs -
        df - DataFrame
    Returns -
        DataFrame
    """
    return df.astype(
        {
            'year'     : 'int',
            'W_TvT'    : 'int',
            'L_TvT'    : 'int',
            'T_TvT'    : 'int',
            'OTL_TvT'  : 'int',
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
            'hW'       : 'int',
            'hL'       : 'int',
            'hT'       : 'int',
            'hOTL'     : 'int',
            'rW'       : 'int',
            'rL'       : 'int',
            'rT'       : 'int',
            'rOTL'     : 'int',
        }
    )

def only_nhl(df):
    """Restrict data to only the NHL.

    Inputs - 
        df - DataFrame
    Returns -
        DataFrame
    """
    return df[df.lgID == 'NHL']

def conferences_and_divisions(df):
    """Restrict to years in which both conferences and divisions existed.

    Inputs -
        df - DataFrame
    Returns -
        DataFrame
    """
    return df[df.year >= 1974]

def winning_percentage(df, w, l, t, otl):
    """Calculate the winning percentage taking into account the possibility of ties.

    Inputs -
        df   - DataFrame
        w    - wins column name
        l    - losses column name
        t    - ties column name
        otl  - overtime losses column name
    Returns -
        list of floats
    """
    percentages = []
    for i in range(len(df[w])):
        n_wins = df[w][i]
        n_loss = df[l][i]
        n_ties = df[t][i]
        n_otls = df[otl][i]
        if n_ties < 0 and n_otls < 0:
            logger.error('found a problem! {df[w][i]}, {df[l][i]}, {df[t][i]}, {df[otl][i]}')
            exit(1)

        if n_ties >= 0:
            if n_wins + n_loss + n_ties == 0:
                percentages.append(-1.000)
            else:
                percentages.append( round((2*n_wins + n_ties) / (2*(n_wins+n_loss+n_ties)), 3) )
        elif n_otls >= 0:
            if n_wins + n_loss + n_otls == 0:
                percentages.append(-1.000)
            else:
                percentages.append( round(n_wins / (n_wins+n_loss+n_otls), 3) )

    return percentages

def add_winning_percentages(df):
    """Add extra winning percentage columns

    Inputs -
        df - DataFrame
    Returns -
        DataFrame
    """
    tmp = df.copy(deep=True)

    tmp['WP_TvT'] = winning_percentage(df, 'W_TvT', 'L_TvT', 'T_TvT', 'OTL_TvT')
    tmp['WP_split'] = winning_percentage(df, 'W_split', 'L_split', 'T_split', 'OTL_split')
    tmp['WP_home'] = winning_percentage(df, 'hW', 'hL', 'hT', 'hOTL')
    tmp['WP_road'] = winning_percentage(df, 'rW', 'rL', 'rT', 'rOTL')

    return tmp

def transform(df):
    """Transform ingested data.

    Inputs -
        df - DataFrame from extract.extract()
    Returns -
        DataFrame
    """
    # Transformation 1 - drop unwanted column types
    try:
        df = drop_unwanted_cols(df)
    except KeyError as e:
        logger.error(f'Unable to find all columns to drop in transformation: {e}')
        exit(1)
    logger.info('Successfully dropped unwanted columns')

    # Transformation 2 - replace missing data with context specific values
    df = replace_missing_data(df)
    logger.info('Successfully replaced missing data')

    # Transformation 3 - specify column types for consistency
    try:
        df = specify_column_types(df)
    except ValueError as e:
        logger.error(f'Unable to change column type: {e}')
        exit(1)
    logger.info('Successfully specified column types')

    # Transformation 4 - restrict to only the NHL
    df = only_nhl(df)
    logger.info('Successfully dropped any non-NHL entries')

    # Transformation 5 - restrict to years when both conferences and divisions existed
    df = conferences_and_divisions(df)
    logger.info('Successfully selected years with both conferences and divisions')

    # Reset index after before last transformation for looping through rows
    df.reset_index(drop=True, inplace=True)

    #  Transformation 6 - add winning percentage columns
    df = add_winning_percentages(df)
    logger.info('Successfully calculated winning percentages')

    return df
