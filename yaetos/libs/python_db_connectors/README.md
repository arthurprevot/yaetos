# python_db_connectors
Runs queries from python, oracle and hive for now, and outputs a pandas dataframe.

Usage :
```
from query_oracle import query
df = query('select * from some_oracle_table', db=name_of_connection_from_credentials.cfg)
```

## Setup
Requires renaming 'credentials.cfg.example' to 'credentials.cfg' and adding user/passwords.
