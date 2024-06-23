import pandas as pd
import os
from time import time
from compare_pandas_dfs import compare_dfs_exact


def query_and_cache(query_str, name, folder, to_csv_args={}, dbargs={}, db_type='oracle', force_rerun=False, show=False):
    (name, fname_base, fname_csv, fname_pykl, fname_sql) = filename_expansion(name, folder)
    if not os.path.isfile(fname_pykl) or force_rerun:
        print("Running query: ", name, '\n', query_str)
        query = query_selector(db_type)
        start_time = time()
        df = query(query_str, **dbargs)
        end_time = time()
        elapsed = end_time - start_time
        print("Ran in {} s".format(elapsed))
        if show:
            print(df)
        drop_if_needed(df, name, folder, to_csv_args, db_type, elapsed, query_str, force_rerun)
    else:
        df = pd.read_pickle(fname_pykl)
        print("Loaded from file: ", fname_pykl)
    return df


def process_and_cache(name, folder, func, to_csv_args={}, force_rerun=False, show=False, **func_args):
    # code here mostly duplicated from query_and_cache, TODO: get better way
    (name, fname_base, fname_csv, fname_pykl, fname_sql) = filename_expansion(name, folder)
    if not os.path.isfile(fname_pykl) or force_rerun:
        print("Running output: ", folder + name, ", funct: ", func)  # , 'with args', func_args
        start_time = time()
        df = func(**func_args)
        end_time = time()
        elapsed = end_time - start_time
        print("Ran in {} s".format(elapsed))
        if show:
            print(df)
        drop_if_needed(df, name, folder, to_csv_args, force_rerun=force_rerun)
    else:
        df = pd.read_pickle(fname_pykl)
        print("Loaded from file: ", fname_pykl)
    return df


def drop_if_needed(df, name, folder, to_csv_args, db_type='n/a', elapsed='n/a', query_str='n/a', force_rerun=True):
    (name, fname_base, fname_csv, fname_pykl, fname_sql) = filename_expansion(name, folder)
    prev_file_exist = os.path.isfile(fname_pykl)
    if force_rerun and prev_file_exist:
        df_pre = pd.read_pickle(fname_pykl)
        is_same = compare_dfs_exact(df_pre, df)
        if is_same:
            print("Output is the same as the previous one (from '{}') so not re-dropping it.".format(fname_pykl))
            # TODO: should still drop the sql file in case it has changed.
        else:
            option = ask_user(is_same, fname_pykl)
            if option == 'overwrite':
                drop_files(df, fname_pykl, fname_csv, fname_sql, name, db_type, elapsed, query_str, to_csv_args)
            elif option == 'new_name':
                fname_pykl = fname_base + "_4debug.pykl"
                fname_csv = fname_base + "_4debug.csv"
                fname_sql = fname_base + "_4debug.sql"
                drop_files(df, fname_pykl, fname_csv, fname_sql, name, db_type, elapsed, query_str, to_csv_args)
            else:
                print("Didn't drop the files.")
    else:
        drop_files(df, fname_pykl, fname_csv, fname_sql, name, db_type, elapsed, query_str, to_csv_args)


def filename_expansion(name, folder):
    if name.endswith('.csv'):
        name = name[:-4]
    fname_base = folder + name
    fname_pykl = fname_base + ".pykl"
    fname_csv = fname_base + ".csv"
    fname_sql = fname_base + ".sql"
    return name, fname_base, fname_csv, fname_pykl, fname_sql


def drop_files(df, fname_pykl, fname_csv, fname_sql, name, db_type, elapsed, query_str, to_csv_args={}):
    df.to_pickle(fname_pykl)
    kwargs = {'sep': ';', 'encoding': 'utf8', 'decimal': '.'}
    kwargs.update(to_csv_args)
    df.to_csv(fname_csv, **kwargs)  # Ex to_csv_args: index=False, decimal=',' (for EU), encoding='utf8', or encoding='cp1252'
    content = "-- name: %s\n-- db_creds (#db_type#): %s\n-- time (s): %s\n-- query: \n%s\n-- end" % (name, db_type, elapsed, query_str)
    write_file(fname_sql, content)
    print("Wrote table to files: ", fname_pykl, fname_csv, fname_sql)


def ask_user(is_identical, fname_pykl):
    print("Output is different from the previous one ('{}'), do you want to:".format(fname_pykl))
    print('[1] Overwrite it')
    print("[2] Save as different name")
    print("[3] Don't save")
    answer = input('Your choice ? ')
    options = {1: 'overwrite', 2: 'new_name', 3: 'ignore'}
    return options[int(answer)]


def query_selector(db_type):
    # only import dependencies when needed as they involve installations of external tools/libs.
    if db_type == 'oracle':
        from libs.python_db_connectors.query_oracle import query as query_oracle
        return query_oracle
    elif db_type == 'mysql':
        from libs.python_db_connectors.query_mysql import query as query_mysql
        return query_mysql
    elif db_type == 'redshift':
        from libs.python_db_connectors.query_redshift import query as query_redshift
        return query_redshift
    elif db_type == 'hive':
        from libs.python_db_connectors.query_hive import query as query_hive
        return query_hive
    elif db_type == 'salesforce':
        from libs.python_db_connectors.query_salesforce import query as query_sf
        return query_sf
    elif db_type == 'pandasql':
        from query_pandasql import query_pandasql
        return query_pandasql
    elif db_type == 'sparksql_local':
        from query_sparksql_local import query_sparksql_local
        return query_sparksql_local
    else:
        raise "unsupported db type: %s" % db_type  # TODO: fix raise.


def write_file(fname, content):
    fh = open(fname, 'w')
    fh.write(content)
    fh.close()


if __name__ == "__main__":
    df = query_and_cache('SELECT * \nFROM all_tables', name='testAP', folder='tempo/', db='name_of_your_db')
