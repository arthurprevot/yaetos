from pyhive import hive
import pandas as pd
from ConfigParser import ConfigParser
import os


def connect(db):
    config = ConfigParser()
    config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'credentials.cfg'))

    params = {
        'user':     config.get(db, 'user'),
        'host':     config.get(db, 'host'),
        'port':     config.get(db, 'port') if config.has_option(db, 'port') else "10000",
        }
    return hive.Connection(host=params['host'], port=params['port'], username=params['user'])


def query(query_str, **kwarg):
    """
    Run an sql query in oracle and return output to pandas table (dataframe).
    """
    connection = connect(**kwarg)
    cursor = connection.cursor()

    try:
        cursor.execute(query_str)
    except :
        raise 'Failed running query'

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame.from_records(cursor.fetchall(), columns=columns)
    connection.close()
    return df

if __name__ == "__main__":
    df = query("SHOW TABLES", db='name_of_connection_from_credentials_file')
    print(df)
