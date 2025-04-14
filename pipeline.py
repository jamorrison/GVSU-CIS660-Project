import logging

from extract import extract
from transform import transform

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
    # TODO: fill out loading of data into database

    return None

if __name__ == '__main__':
    pipeline()
