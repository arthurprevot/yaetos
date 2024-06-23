import pandas as pd
import numpy as np
import hashlib


def compare_dfs_exact(df1, df2):
    print('Looking into diffs between previous and new dataset.')
    # TODO: check columns before checking content.
    try:
        hash1 = hashlib.sha256(pd.util.hash_pandas_object(df1, index=True).values).hexdigest()
        hash2 = hashlib.sha256(pd.util.hash_pandas_object(df2, index=True).values).hexdigest()
        is_identical = hash1 == hash2
    except Exception:
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


def compare_dfs_fuzzy(df1, pks1, compare1, df2, pks2, compare2, strip=True, filter_deltas=True, threshold=0.01):
    """Note: Doesn't support both dfs having same column names (or at least the ones that need to be compared.)"""
    print('Length df1', len(df1), '. Unique values in each field :\n', df1[pks1].nunique())
    print('Length df2', len(df2), '. Unique values in each field :\n', df2[pks2].nunique())

    if strip:
        df1 = df1[pks1 + compare1]
        df2 = df2[pks2 + compare2]

    # Join datasets
    df_joined = pd.merge(left=df1, right=df2, how='outer', left_on=pks1, right_on=pks2, indicator=True, suffixes=('_1', '_2'))
    print('Length df_joined (before filtering if any)', len(df_joined))
    df_joined['_no_deltas'] = df_joined['_merge'].apply(lambda cell: True if cell == 'both' else False)  # init to True for 'both'. To be updated later.

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
            return np.abs(np.divide((row[item1] - row[item2]), float(row[item1]))) * 100

    # Check deltas (numerical values)
    np.seterr(divide='ignore')  # to handle the division by 0 in divide().
    for ii in range(len(compare1)):
        item1 = compare1[ii]
        item2 = compare2[ii]
        if item1 == item2:  # check necessary for next step. See comment below.
            item1 = item1 + '_1'
            item2 = item2 + '_2'
        is_numeric_1 = pd.api.types.is_numeric_dtype(df_joined[item1])
        is_numeric_2 = pd.api.types.is_numeric_dtype(df_joined[item2])
        is_str_1 = pd.api.types.is_string_dtype(df_joined[item1])
        is_str_2 = pd.api.types.is_string_dtype(df_joined[item2])
        print(f'Starting to compare column ({item1} and {item2}), with types ({df_joined[item1].dtype} and {df_joined[item2].dtype})')
        if is_numeric_1 and is_numeric_2:
            try:
                df_joined['_delta_' + item1] = df_joined.apply(lambda row: (row[item1] if not pd.isna(row[item1]) else 0.0) - (row[item2] if not pd.isna(row[item2]) else 0.0), axis=1)
                df_joined['_delta_' + item1 + '_%'] = df_joined.apply(check_delta, axis=1)
                df_joined['_no_deltas'] = df_joined.apply(lambda row: row['_no_deltas'] is True and row['_delta_' + item1 + '_%'] < threshold, axis=1)
                print(f"Column summary, all_equal = {df_joined['_delta_' + item1 + '_%'].apply(lambda cell: cell < threshold).all()}. within treshold of {threshold}")
            except Exception as err:
                raise Exception("Failed item={}, error: \n{}".format(item1, err))
        elif is_str_1 and is_str_2:
            df_joined['_delta_' + item1 + '_equal'] = df_joined.apply(lambda row: row[item1] == row[item2], axis=1)
            df_joined['_no_deltas'] = df_joined.apply(lambda row: row['_no_deltas'] is True and row['_delta_' + item1 + '_equal'], axis=1)
            print(f"Column summary, all_equal = {df_joined['_delta_' + item1 + '_equal'].all()}.")
        else:
            try:
                df_joined['_delta_' + item1 + '_equal'] = df_joined.apply(lambda row: row[item1] == row[item2], axis=1)
                df_joined['_no_deltas'] = df_joined.apply(lambda row: row['_no_deltas'] is True and row['_delta_' + item1 + '_equal'], axis=1)
                print(f"Column summary, all_equal = {df_joined['_delta_' + item1 + '_equal'].all()}.")
            except Exception:
                df_joined['_no_deltas'] = df_joined.apply(lambda row: row['_no_deltas'] is False, axis=1)
            print(f'Column summary, all_equal = False. The columns to compare (i.e. {item1} and {item2}) have mismatched types, or are not numerical nor strings.')

    np.seterr(divide='raise')
    if filter_deltas:
        df_joined = df_joined[df_joined.apply(lambda row: row['_no_deltas'] is False, axis=1)].reset_index()
        print('Length df_joined (post filtering)', len(df_joined))
    df_joined.sort_values(by=['_merge'] + pks1, inplace=True)
    return df_joined
