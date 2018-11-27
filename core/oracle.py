import sys; sys.path.append('/Users/aprevot/Biz_Schibsted/code/analysis_toolkit/')
import sys; sys.path.append('/Users/aprevot/Biz_Schibsted/code/python_db_connectors/')
from query_helper import query_and_cache, filename_expansion
from query_oracle import connect, query
import pandas as pd
import numpy as np
from sqlalchemy import types

def create_table(df, connection_profile, name_tb, types):
    connection = connect(db=connection_profile, connection_type='sqlalchemy')

    # varchar_length = types.VARCHAR(df_csv_list['tmp_id'].str.len().max())
    df.to_sql(name=name_tb, con=connection, if_exists='replace',
              # dtype={'tmp_id': varchar_length}
              dtype=types
              )  # dtype necessary to avoid having CLOB type by default (limit comparison with other strings.).
              # 'month': types.Date
              # types.Integer()
    print "Created table", name_tb, ", using connection profile", connection_profile
