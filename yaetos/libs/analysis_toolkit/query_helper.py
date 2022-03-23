import pandas as pd
import numpy as np
import os
from time import time
import hashlib


def query_and_cache(query_str, name, folder, to_csv_args={}, dbargs={}, db_type='oracle', force_rerun=False, show=False):
    (name, fname_base, fname_csv, fname_pykl, fname_sql) = filename_expansion(name, folder)
    if not os.path.isfile(fname_pykl) or force_rerun:
        print("Running query: ", name, '\n', query_str)
        query = query_selector(db_type)
        start_time = time()
        df = query(query_str, **dbargs)
        end_time = time()
        elapsed = end_time - start_time
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
        print("Running output: ", folder+name, ", funct: ", func) #, 'with args', func_args
        start_time = time()
        df = func(**func_args)
        end_time = time()
        elapsed = end_time - start_time
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
        is_same = diff_dfs(df_pre, df)
        if is_same:
            print("Output is the same as the previous one (from '{}') so not re-dropping it.".format(fname_pykl))
            #TODO: should still drop the sql file in case it has changed.
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

def diff_dfs(df1, df2):
    print('Looking into diffs between previous and new dataset.')
    try:
        hash1 = hashlib.sha256(pd.util.hash_pandas_object(df1, index=True).values).hexdigest()
        hash2 = hashlib.sha256(pd.util.hash_pandas_object(df2, index=True).values).hexdigest()
        is_identical = hash1 == hash2
    except:
        print("Diff computation failed so assuming files are not similar.")
        return False

    if is_identical:
        print('dfs are the same.')
        return True

    print('dfs are different.')

    diff_columns = list(set(df1.columns) - set(df2.columns))
    if diff_columns:
        print('diff_columns: ', diff_columns)
        return False

    print('columns are the same.')
    return False

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
    kwargs = {'sep':';', 'encoding':'utf8', 'decimal':'.'}
    kwargs.update(to_csv_args)
    df.to_csv(fname_csv, **kwargs)  # Ex to_csv_args: index=False, decimal=',' (for EU), encoding='utf8', or encoding='cp1252'
    content = "-- name: %s\n-- db_creds (#db_type#): %s\n-- time (s): %s\n-- query: \n%s\n-- end"%(name, db_type, elapsed, query_str)
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
        raise "unsupported db type: %s"%db_type  # TODO: fix raise.

def write_file(fname, content):
    fh = open(fname, 'w')
    fh.write(content)
    fh.close()

def compare_dfs(df1, pks1, compare1, df2, pks2, compare2, strip=True, filter_deltas=True, threshold=0.01):
    """Note: Doesn't support both dfs having same column names (or at least the ones that need to be compared.)"""
    print('Length df1', len(df1), df1[pks1].nunique())
    print('Length df2', len(df2), df2[pks2].nunique())

    if strip :
        df1 = df1[pks1+compare1]
        df2 = df2[pks2+compare2]

    # Join datasets
    df_joined = pd.merge(left=df1, right=df2, how='outer', left_on=pks1, right_on=pks2, indicator = True, suffixes=('_1', '_2'))
    print('Length df_joined', len(df_joined))
    df_joined['_no_deltas'] = True # init

    def check_delta(row):
        if pd.isna(row[item1]) and pd.isna(row[item2]):
            return 0
        elif pd.isna(row[item1]) or pd.isna(row[item2]):
            return 100
        elif float(row[item1]) == 0 and float(row[item2]) == 0:
            return 0
        elif float(row[item1]) == 0 or float(row[item2]) == 0:
            return 100
        else:
            return np.abs(np.divide((row[item1]-row[item2]), float(row[item1])))*100

    # Check deltas
    np.seterr(divide='ignore')  # to handle the division by 0 in divide().
    for ii in range(len(compare1)):
        item1 = compare1[ii]
        item2 = compare2[ii]
        assert item1!=item2  # necessary for next step. See comment below.
        try:
            df_joined['_delta_'+item1] = df_joined.apply(lambda row: (row[item1] if not pd.isna(row[item1]) else 0.0)-(row[item2] if not pd.isna(row[item2]) else 0.0), axis=1) # need to deal with case where df1 and df2 have same col name and merge adds suffix _1 and _2
            df_joined['_delta_'+item1+'_%'] = df_joined.apply(check_delta, axis=1)
            df_joined['_no_deltas'] = df_joined.apply(lambda row: row['_no_deltas']==True and row['_delta_'+item1+'_%']<threshold, axis=1)
        except Exception as err:
            raise Exception("Failed item={}, error: \n{}".format(item1, err))


    np.seterr(divide='raise')
    if filter_deltas:
        df_joined = df_joined[df_joined.apply(lambda row : row['_no_deltas']==False, axis=1)].reset_index()
    df_joined.sort_values(by=['_merge']+pks1, inplace=True)
    return df_joined


if __name__ == "__main__":
    df = query_and_cache('SELECT * \nFROM all_tables', name='testAP', folder='tempo/', db='name_of_your_db')
