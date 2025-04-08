from extract import extract

def pipeline():
    """Main data pipeline

    Inputs -
        None
    Returns -
        None
    """
    # Extract
    df = extract()
    print(df)

    # Transform
    # TODO: fill out transformation of data

    # Load
    # TODO: fill out loading of data into database

    return None

if __name__ == '__main__':
    pipeline()
