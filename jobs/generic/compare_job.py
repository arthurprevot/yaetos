from yaetos.etl_utils import ETL_Base, Commandliner
from yaetos.libs.analysis_toolkit.compare_pandas_dfs import compare_dfs_fuzzy, compare_dfs_exact
import yaetos.pandas_utils as pu
import pandas as pd


class Job(ETL_Base):

    def transform(self, tableA, tableB):
        pksA = self.jargs.inputs['tableA']['pk']
        pksB = self.jargs.inputs['tableB']['pk']
        return compare_dfs(tableA, tableB, pksA, pksB)


def compare_dfs(tableA, tableB, pksA, pksB):
    # Comparing columns
    diff_columns = list(set(tableA.columns) - set(tableB.columns))
    common_columns = list(set(tableA.columns) & set(tableB.columns))

    if diff_columns:
        print('datasets are different')
        print('The deltas in columns are: ', diff_columns)
        print('The columns in common are: ', common_columns)
        print('Rest of the comparison will be based on columns in common')

    tableA = tableA[common_columns]  # applied even if columns are all the same to make sure they are in the same order (impt for checks below)
    tableB = tableB[common_columns]

    # Comparing datasets length
    diff_row_count = len(tableA) != len(tableB)
    if diff_row_count:
        print('datasets have different row count')
        print('Length TableA: ', len(tableA))
        print('Length TableB: ', len(tableB))

    # Comparing dataset content, exact
    is_identical = compare_dfs_exact(tableA, tableB)
    if is_identical:
        message = 'datasets are identical' if not diff_columns else 'datasets (with columns in common) are identical'
        print(message)
        return pd.DataFrame()

    # Comparing dataset content, fuzzy
    compare1 = list(set(tableA.columns) - set(pksA))
    compare2 = list(set(tableB.columns) - set(pksB))
    strip = False
    filter_deltas = True

    if not pu.check_pk(tableA, pksA):
        print('The chosen PKs are not actual PKs (i.e. not unique). Retry with modified PK inputs.')
        return pd.DataFrame()
    elif not pu.check_pk(tableB, pksB):
        print('The chosen PKs are not actual PKs (i.e. not unique). Retry with modified PK inputs.')
        return pd.DataFrame()

    print('About to compare, column by column.')
    df_out = compare_dfs_fuzzy(tableA, pksA, compare1, tableB, pksB, compare2, strip, filter_deltas, threshold=0.01)
    print('Finishing compare, column by column.')

    return df_out


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
