"""
Job to showcase loading file as pandas dataframe instead of spark dataframe.
This allows for faster run for small datasets but looses some of the benefits of spark dataframes (support for SQL, better field type management, etc.).
Transformation is the same as ex1_sql_job.sql
"""
from yaetos.etl_utils import ETL_Base, Commandliner
import pandas as pd
import numpy as np


class Job(ETL_Base):
    def transform(self, some_events, other_events):
        some_events = some_events[:1000]  # to speed up since it is an example job
        df1 = some_events[some_events.apply(lambda row: row['action'] == 'searchResultPage' and float(row['n_results']) > 0, axis=1)]
        df2 = pd.merge(left=df1, right=other_events, how='inner', left_on='session_id', right_on='session_id', indicator=True, suffixes=('_1', '_2'))
        df3 = df2.groupby(by=['session_id']).agg({'_merge': np.count_nonzero})
        df3.rename(columns={'_merge': 'count_events'}, inplace=True)
        df3.sort_values('count_events', ascending=False, inplace=True)
        df3.reset_index(drop=False, inplace=True)
        self.logger.info('Post filter length: {}'.format(len(df1)))
        return df3


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
