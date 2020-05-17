from simple_salesforce import Salesforce
import pandas as pd
from configparser import ConfigParser
import os


def connect(creds_section):
    config = ConfigParser()
    config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'credentials.cfg'))
    user = config.get(creds_section, 'user')
    pwd = config.get(creds_section, 'password')
    return Salesforce(username=user, password=pwd, security_token='')

def query(query_str, creds_section):
    sf = connect(creds_section)
    resp = sf.query_all(query_str)
    rows = resp['records']
    for row in rows:
        row.__delitem__('attributes')
    df = pd.DataFrame.from_dict(rows)
    return df

if __name__ == "__main__":
    df = query('SELECT Account.Name FROM Account', creds_section='salesforce_aprevot')
    print(df)
