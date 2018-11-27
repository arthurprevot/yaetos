import sys; sys.path.append('/Users/aprevot/Biz_Schibsted/code/analysis_toolkit/')
from query_oracle import connect, query

def create_table(df, connection_profile, name_tb, types):
    """
    Create table, full drop everytime.
    types should be of sqlalchemy type.
    Ex: types.Date(), types.Integer()
    """
    connection = connect(db=connection_profile, connection_type='sqlalchemy')
    df.to_sql(name=name_tb, con=connection, if_exists='replace',
              dtype=types)  # dtype necessary to avoid having CLOB type by default (limit comparison with other strings.).
    print "Copied table to oracle ", name_tb, ", using connection profile", connection_profile


if __name__ == '__main__':
    from sqlalchemy import types
    types = {
        'session_id': types.VARCHAR(16),
        'count_events': types.Integer(),
        }
