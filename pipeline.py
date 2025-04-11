import logging

from extract import extract

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
    print(df)

    # Transform
    # TODO: fill out transformation of data

    # Load
    # TODO: fill out loading of data into database

    return None

if __name__ == '__main__':
    pipeline()
