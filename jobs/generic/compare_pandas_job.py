from yaetos.etl_utils import ETL_Base, Commandliner
from yaetos.libs.analysis_toolkit.query_helper import compare_dfs, diff_dfs
import pandas as pd


class Job(ETL_Base):

    def transform(self, tableA, tableB):
        tableA['action'].iloc[2] = 'other action'  # only for testing.

        # Comparing columns
        diff_columns = list(set(tableA.columns) - set(tableB.columns))
        common_columns = list(set(tableA.columns) & set(tableB.columns))

        if diff_columns:
            print('datasets are different')
            print('The deltas in columns are: ', diff_columns)
            print('The columns in common are: ', common_columns)
            print('Rest of the comparison will be based on columns in common')
            tableA = tableA[common_columns]
            tableB = tableB[common_columns]

        # Comparing datasets length
        diff_row_count = len(tableA) != len(tableB)
        if diff_row_count:
            print('datasets have different row count')
            print('Length TableA: ', len(tableA))
            print('Length TableB: ', len(tableB))

        # Comparing dataset content, exact
        is_identical = diff_dfs(tableA, tableB)
        if is_identical:
            message = 'datasets are identical' if not diff_columns else 'datasets (with columns in common) are identical'
            print(message)
            df = pd.DataFrame()
            return df

        # Comparing dataset content, fuzzy 
        pks1 = self.jargs.inputs['tableA']['pk']
        pks2 = self.jargs.inputs['tableB']['pk']
        compare1 = list(set(tableA.columns) - set(pks1))
        compare2 = list(set(tableB.columns) - set(pks2))
        strip = False
        filter_deltas = True

        if not self.check_pk(tableA, pks1, df_type='pandas'):
            print('The select PKs are not real PKs. Retry with modified PK input.')
            return pd.DataFrame()
        elif not self.check_pk(tableB, pks2, df_type='pandas'):
            print('The select PKs are not real PKs. Retry with modified PK input.')
            return pd.DataFrame()

        print('About to compare, column by column.')
        df_out = compare_dfs(tableA, pks1, compare1, tableB, pks2, compare2, strip, filter_deltas, threshold=0.01)
        print('Finishing compare, column by column.')

        # import ipdb; ipdb.set_trace()
        return df_out


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
