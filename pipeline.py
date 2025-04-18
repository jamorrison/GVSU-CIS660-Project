import logging

from extract import extract
from transform import transform
from loading import load
from data_analysis import do_analysis

logger = logging.getLogger('pipeline')

def pipeline():
    """Main data pipeline

    Inputs -
        None
    Returns -
        None
    """
    # Set up logging
    FORMAT = '[{levelname} - {name} - {asctime}] {message}'
    logging.basicConfig(format=FORMAT, style='{', level=logging.INFO)

    # Extract
    logger.info('Start extraction')
    df = extract()
    logger.info('Finished extraction')

    # Transform
    logger.info('Start transformation')
    df = transform(df)
    logger.info('Finished transformation')

    # Load
    logger.info('Start database loading')
    load(df)
    logger.info('Finished database loading')

    # Data analysis and visualization
    logger.info('Start data analysis')
    do_analysis()
    logger.info('Finished data analysis')

    return None

if __name__ == '__main__':
    pipeline()
