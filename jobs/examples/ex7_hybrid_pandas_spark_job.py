"""Same transformation as ex1_sql_job.sql, done in pandas"""
from yaetos.etl_utils import ETL_Base, Commandliner
from yaetos.db_utils import pdf_to_sdf
from sqlalchemy import types
import pandas as pd
import numpy as np


class Job(ETL_Base):
    OUTPUT_TYPES = {
        'session_id': types.VARCHAR(100),
        'count_events': types.INT()}

    def transform(self, some_events, other_events):
        # Conversions of inputs to pandas
        some_events_pd = some_events.toPandas()
        other_events_pd = other_events.toPandas()

        # Transformation in pandas
        some_events_pd = some_events_pd[:1000]
        df1 = some_events_pd[some_events_pd.apply(lambda row: row['action'] == 'searchResultPage' and float(row['n_results']) > 0, axis=1)]
        df2 = pd.merge(left=df1, right=other_events_pd, how='inner', left_on='session_id', right_on='session_id', indicator=True, suffixes=('_1', '_2'))
        df3 = df2.groupby(by=['session_id']).agg({'_merge': np.count_nonzero})
        df3.rename(columns={'_merge': 'count_events'}, inplace=True)
        df3.sort_values('count_events', ascending=False, inplace=True)
        df3.reset_index(drop=False, inplace=True)
        self.logger.info('Post filter length: {}'.format(len(df1)))

        # Conversion of output to Spark
        sdf = pdf_to_sdf(df3, self.OUTPUT_TYPES, self.sc, self.sc_sql)
        return sdf


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
