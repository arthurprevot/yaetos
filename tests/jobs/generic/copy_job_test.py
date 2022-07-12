from jobs.generic.copy_job import Job
import pandas as pd


class Test_Job(object):
    def test_transform_spark(self, sc, sc_sql, ss, get_pre_jargs):
        table = [
            {'session_id': 1},
            {'session_id': 12}]

        table_to_copy = ss.read.json(sc.parallelize(table))
        loaded_inputs = {'table_to_copy': table_to_copy}
        actual = Job(pre_jargs=get_pre_jargs(loaded_inputs.keys())).etl_no_io(sc, sc_sql, loaded_inputs=loaded_inputs)[0].toPandas().to_dict(orient='records')
        expected = table
        assert actual == expected

    def test_transform_pandas(self, get_pre_jargs):
        table = [
            {'session_id': 1},
            {'session_id': 12}]

        table_to_copy = pd.DataFrame(table)
        loaded_inputs = {'table_to_copy': table_to_copy}
        pre_jargs = get_pre_jargs(loaded_inputs.keys())
        pre_jargs['defaults_args']['output']['df_type'] = 'pandas'
        actual = Job(pre_jargs=pre_jargs).etl_no_io(sc=None, sc_sql=None, loaded_inputs=loaded_inputs)[0].to_dict(orient='records')

        expected = table
        assert actual == expected
