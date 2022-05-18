"""Job to showcase loading file as pandas dataframe instead of spark dataframe. This allows for faster run for small datasets but looses some of the benefits of spark dataframes (support for SQL, better field type management, etc.)."""
from yaetos.etl_utils import ETL_Base, Commandliner #, pdf_to_sdf
import pandas as pd
import numpy as np

class Job(ETL_Base):
    # OUTPUT_TYPES = {
    #     'some_field1': types.INT(),
    #     'some_field2': types.INT(),
    #     'some_field3': types.VARCHAR(100)}

    def transform(self, some_events, other_events):
        some_events = some_events[:10000]
        df1 = some_events[some_events.apply(lambda row: row['action'] == 'searchResultPage' and row['n_results']>0, axis=1)]
        df2 = pd.merge(left=df1, right=other_events, how='inner', left_on='session_id', right_on='session_id', indicator = True, suffixes=('_1', '_2'))

        # import ipdb; ipdb.set_trace()
        df3 = df2.groupby(by=['session_id']).agg({'_merge': np.count_nonzero})
        self.logger.info('-----asdf')

            # SELECT se.session_id, count(*) as count_events
            # FROM some_events se
            # JOIN other_events oe on se.session_id=oe.session_id
            # WHERE se.action='searchResultPage' and se.n_results>0
            # group by se.session_id
            # order by count(*) desc


        # sdf = pdf_to_sdf(pdf, self.OUTPUT_TYPES, self.sc, self.sc_sql)
        return df3


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
