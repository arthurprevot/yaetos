"""To show koalas API. Same process as SQL code in ex2_frameworked_job.py"""
from core.etl_utils import ETL_Base, Commandliner
import pandas as pd
import numpy as np
import databricks.koalas as ks

class Job(ETL_Base):
    def transform(self, some_events, other_events):
        # Convert spark df to koalas df
        se_kdf = some_events.to_koalas()
        oe_kdf = other_events.to_koalas()

        # processing
        se_kdf = se_kdf[se_kdf['action']=='searchResultPage']
        se_kdf = se_kdf[se_kdf['n_results']>0]
        merged_kdf = ks.merge(se_kdf, oe_kdf, on='session_id', how='inner', suffixes=('_l','_r'))
        grouped = merged_kdf.groupby(by=['session_id']).count()

        # back to spark df
        sdf = grouped.to_spark()
        return sdf


if __name__ == "__main__":
    args = {
        'job_param_file':   'conf/jobs_metadata.yml',  # Just to be explicit. Not needed since this is default.
        'connection_file':  'conf/connections.cfg',  # Just to be explicit. Not needed since this is default.
        'aws_config_file':  'conf/aws_config.cfg',  # Just to be explicit. Not needed since this is default.
        'aws_setup':        'dev',  # Just to be explicit. Not needed since this is default.
        'jobs_folder':      'jobs/',  # Just to be explicit. Not needed since this is default.
        }
    Commandliner(Job, **args)
