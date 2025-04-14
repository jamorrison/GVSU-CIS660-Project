import logging
import pandas as pd

logger = logging.getLogger('transform')

# Implement at least three data transformation steps. These could include data cleansing, normalization, aggregation, and/or enrichment.
#     - Cleaning & Preprocessing: Handle missing values, duplicates, data type inconsistencies/duplicates.
#     - Normalization / Aggregation: Standardize columns (e.g., numeric scaling), group data for summary.
#     - Feature Engineering: Create at least one new feature or variable that adds value (e.g., a ratio, categorization, or date-time extraction)..

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

def transformation_2(df):
    """Drop any rows with 1 or more NAs

    Inputs -
        df - DataFrame
    Returns -
        DataFrame
    """
    return df.dropna(ignore_index=True)

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

    # Reset index after transformations
    df.reset_index(drop=True, inplace=True)

    return df
