import pandas as pd
import os
from time import time
import hashlib


def query_and_cache(query_str, name, folder, db_type='oracle', force_rerun=False, show=False, **dbargs):
    (name, fname_base, fname_csv, fname_pykl, fname_sql) = filename_expansion(name, folder)
    if not os.path.isfile(fname_pykl) or force_rerun:
        print "Running query: ", name, '\n', query_str
        query = query_selector(db_type)
        start_time = time()
        df = query(query_str, **dbargs)
        end_time = time()
        elapsed = end_time - start_time
        drop_if_needed(df, name, folder, db_type, elapsed, query_str, force_rerun)
    else:
        df = pd.read_pickle(fname_pykl)
        print "Loaded from file: ", fname_pykl
    if show:
        print df
    return df

def process_and_cache(name, folder, func, force_rerun=False, show=False, **func_args):
    # code here mostly duplicated from query_and_cache, TODO: get better way
    (name, fname_base, fname_csv, fname_pykl, fname_sql) = filename_expansion(name, folder)
    if not os.path.isfile(fname_pykl) or force_rerun:
        print "Running output: ", folder+name, ", funct: ", func #, 'with args', func_args
        start_time = time()
        df = func(**func_args)
        end_time = time()
        elapsed = end_time - start_time
        drop_if_needed(df, name, folder, force_rerun=force_rerun)
    else:
        df = pd.read_pickle(fname_pykl)
        print "Loaded from file: ", fname_pykl
    if show:
        print df
    return df

def drop_if_needed(df, name, folder, db_type='n/a', elapsed='n/a', query_str='n/a', force_rerun=True):
    (name, fname_base, fname_csv, fname_pykl, fname_sql) = filename_expansion(name, folder)
    prev_file_exist = os.path.isfile(fname_pykl)
    if force_rerun and prev_file_exist:
        df_pre = pd.read_pickle(fname_pykl)
        is_same = diff_dfs(df_pre, df)
        if is_same:
            print "Output is the same as the previous one (from '{}') so not re-dropping it.".format(fname_pykl)
            #TODO: should still drop the sql file in case it has changed.
        else:
            option = ask_user(is_same, fname_pykl)
            if option == 'overwrite':
                drop_files(df, fname_pykl, fname_csv, fname_sql, name, db_type, elapsed, query_str)
            elif option == 'new_name':
                fname_pykl = fname_base + "_4debug.pykl"
                fname_csv = fname_base + "_4debug.csv"
                fname_sql = fname_base + "_4debug.sql"
                drop_files(df, fname_pykl, fname_csv, fname_sql, name, db_type, elapsed, query_str)
            else:
                print "Didn't drop the files."
    else:
        drop_files(df, fname_pykl, fname_csv, fname_sql, name, db_type, elapsed, query_str)

def diff_dfs(df1, df2):
    print 'Looking into diffs between previous and new dataset.'
    try:
        hash1 = hashlib.md5(df1.to_msgpack()).hexdigest()  # crashes when dfs contains unpickeable type.
        hash2 = hashlib.md5(df2.to_msgpack()).hexdigest()  # same
        is_identical = hash1 == hash2
    except:
        print "Diff computation failed so assuming files are not similar."
        return False

    if is_identical:
        print 'dfs are the same.'
        return True

    print 'dfs are different.'

    diff_columns = list(set(df1.columns) - set(df2.columns))
    if diff_columns:
        print 'diff_columns: ', diff_columns
        return False

    print 'columns are the same.'
    return False

def filename_expansion(name, folder):
    if name.endswith('.csv'):
        name = name[:-4]
    fname_base = folder + name
    fname_pykl = fname_base + ".pykl"
    fname_csv = fname_base + ".csv"
    fname_sql = fname_base + ".sql"
    return name, fname_base, fname_csv, fname_pykl, fname_sql

def drop_files(df, fname_pykl, fname_csv, fname_sql, name, db_type, elapsed, query_str):
    df.to_pickle(fname_pykl)
    df.to_csv(fname_csv, sep=';', encoding='utf8', decimal='.')  # TODO: check way to support index=False for some cases, and or delimiter='.', and other encoding (needed 'cp1252' at some point)
    content = "-- name: %s\n-- db_creds (#db_type#): %s\n-- time (s): %s\n-- query: \n%s\n-- end"%(name, db_type, elapsed, query_str)
    write_file(fname_sql, content)
    print "Wrote table to files: ", fname_pykl, fname_csv, fname_sql

def ask_user(is_identical, fname_pykl):
    print "Output is different from the previous one ('{}'), do you want to:".format(fname_pykl)
    print '[1] Overwrite it'
    print "[2] Save as different name"
    print "[3] Don't save"
    answer = raw_input('Your choice ? ')
    options = {1: 'overwrite', 2: 'new_name', 3: 'ignore'}
    return options[int(answer)]

def query_selector(db_type):
    # only import dependencies when needed as they involve installations of external tools/libs.
    if db_type == 'oracle':
        from libs.python_db_connectors.query_oracle import query as query_oracle
        return query_oracle
    elif db_type == 'hive':
        from libs.python_db_connectors.query_hive import query as query_hive
        return query_hive
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


if __name__ == "__main__":
    df = query_and_cache('SELECT * \nFROM all_tables', name='testAP', folder='tempo/', db='name_of_your_db')
