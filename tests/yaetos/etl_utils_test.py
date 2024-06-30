from pandas.testing import assert_frame_equal
import pandas as pd
import numpy as np
import pytest
from yaetos.etl_utils import ETL_Base, \
    Period_Builder, Job_Args_Parser, Job_Yml_Parser, Runner, Flow, \
    get_job_class, \
    LOCAL_JOB_FOLDER, JOBS_METADATA_FILE


class Test_ETL_Base(object):
    def test_check_pk(self, sc, sc_sql, ss):
        sdf = ss.read.json(sc.parallelize([
            {'id': 1},
            {'id': 2},
            {'id': 3}]))

        pks = ['id']
        assert ETL_Base.check_pk(sdf, pks) is True

        sdf = ss.read.json(sc.parallelize([
            {'id': 1},
            {'id': 2},
            {'id': 2}]))

        pks = ['id']
        assert ETL_Base.check_pk(sdf, pks) is False

    def test_set_py_job(self, get_pre_jargs):
        py_job = ETL_Base(pre_jargs=get_pre_jargs({})).set_py_job()
        assert py_job == LOCAL_JOB_FOLDER + 'yaetos/etl_utils.py'  # file is the one that starts execution, typically the job python file.

    def test_load_missing_inputs(self, sc, sc_sql, ss, get_pre_jargs):
        """Confirming load_missing_inputs acts as a passthrough"""
        sdf = ss.read.json(sc.parallelize([
            {'id': 1},
            {'id': 2},
            {'id': 3}]))
        loaded_inputs = {'input1': sdf}
        app_args_expected = loaded_inputs
        assert ETL_Base(pre_jargs=get_pre_jargs(loaded_inputs.keys())).load_missing_inputs(loaded_inputs) == app_args_expected

    def test_get_max_timestamp(self, sc, sc_sql, ss, get_pre_jargs):
        sdf = ss.read.json(sc.parallelize([
            {'id': 1, 'timestamp': '2020-01-01'},
            {'id': 2, 'timestamp': '2020-01-02'},
            {'id': 3, 'timestamp': '2020-01-03'}]))
        pre_jargs_over = {
            'defaults_args': {
                'inputs': {},
                'output': {'inc_field': 'timestamp', 'type': None}}}
        max_timestamp_expected = '2020-01-03'
        assert ETL_Base(pre_jargs=get_pre_jargs(pre_jargs_over=pre_jargs_over)).get_max_timestamp(sdf) == max_timestamp_expected


class Test_Period_Builder(object):
    def test_get_last_day(self):
        from datetime import datetime
        as_of_date = datetime.strptime("2021-01-02", '%Y-%m-%d')
        last_day_real = Period_Builder.get_last_day(as_of_date)
        last_day_expected = "2021-01-01"
        assert last_day_real == last_day_expected

    def test_get_first_to_last_day(self):
        from datetime import datetime
        first_day = "2021-01-01"
        as_of_date = datetime.strptime("2021-01-05", '%Y-%m-%d')
        period_real = Period_Builder.get_first_to_last_day(first_day, as_of_date)
        period_expected = ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04"]
        assert period_real == period_expected

    def test_get_last_output_to_last_day(self):
        from datetime import datetime
        first_day_input = "2021-01-01"
        last_run_period = "2021-01-03"
        as_of_date = datetime.strptime("2021-01-08", '%Y-%m-%d')
        period_real = Period_Builder().get_last_output_to_last_day(last_run_period, first_day_input, as_of_date)
        period_expected = ["2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07"]
        assert period_real == period_expected


class Test_Job_Yml_Parser(object):
    def test_set_py_job_from_name(self):
        py_job = Job_Yml_Parser.set_py_job_from_name('some/job_name.py')
        assert py_job == 'jobs/some/job_name.py'

        py_job = Job_Yml_Parser.set_py_job_from_name('some/job_name')
        assert py_job is None

    def test_set_job_name_from_file(self):
        job_name = Job_Yml_Parser.set_job_name_from_file('jobs/some/file.py')
        assert job_name == 'some/file.py'

        job_name = Job_Yml_Parser.set_job_name_from_file(LOCAL_JOB_FOLDER + 'jobs/some/file.py')
        assert job_name == 'some/file.py'

    def test_set_sql_file_from_name(self):
        sql_file = Job_Yml_Parser.set_sql_file_from_name('some/job_name.sql')
        assert sql_file == 'jobs/some/job_name.sql'

        sql_file = Job_Yml_Parser.set_sql_file_from_name('some/job_name')
        assert sql_file is None

    def test_set_mode_specific_params(self):
        job_name = 'n/a'
        job_param_file = 'conf/jobs_metadata.yml'
        yml_modes = 'dev_EMR'
        skip_job = True
        actual_params = Job_Yml_Parser(job_name, job_param_file, yml_modes, skip_job).set_job_yml(job_name, job_param_file, yml_modes, skip_job)
        expected_params = {
            'aws_config_file': 'conf/aws_config.cfg',
            'aws_setup': 'dev',
            'base_path': '{{root_path}}/pipelines_data',
            'connection_file': 'conf/connections.cfg',
            'email_cred_section': 'some_email_cred_section',
            'emr_core_instances': 0,
            'enable_db_push': False,
            'jobs_folder': 'jobs/',
            'load_connectors': 'all',
            'manage_git_info': True,
            'redshift_s3_tmp_dir': 's3a://dev-spark/tmp_spark/',
            'root_path': 's3://mylake-dev',
            's3_dags': '{{root_path}}/pipelines_metadata/airflow_dags',
            's3_logs': '{{root_path}}/pipelines_metadata',
            'save_schemas': False,
            'schema': 'sandbox',
            'spark_version': '3.5'}
        assert actual_params == expected_params

    def test_set_modes(self):
        yml_modes = 'dev_EMR,your_extra_tenant'  # i.e. using 2 modes
        job_name = 'n/a'
        job_param_file = 'conf/jobs_metadata.yml'
        skip_job = True
        expected_params = {
            'save_schemas': False,
            'other_param': 'some_value'}
        actual_params = Job_Yml_Parser(job_name, job_param_file, yml_modes, skip_job).set_job_yml(job_name, job_param_file, yml_modes, skip_job)
        actual_params = {key: value for (key, value) in actual_params.items() if key in expected_params.keys()}
        assert actual_params == expected_params


class Test_Job_Args_Parser(object):
    def test_no_param_override(self):
        defaults_args = {'py_job': 'some_job.py', 'mode': 'dev_local', 'deploy': 'code', 'output': {'path': 'n/a', 'type': 'csv'}}
        expected_args = {**{'inputs': {}, 'is_incremental': False}, **defaults_args}

        jargs = Job_Args_Parser(defaults_args=defaults_args, yml_args={}, job_args={}, cmd_args={}, build_yml_args=False)
        assert jargs.merged_args == expected_args

    def test_validate_params(self):
        # Error raised, py_job
        defaults_args = {'py_job': None, 'mode': 'dev_local', 'deploy': 'code', 'output': {'path': 'n/a', 'type': 'csv'}}
        job = Job_Args_Parser(defaults_args=defaults_args, yml_args={}, job_args={}, cmd_args={}, build_yml_args=False, validate=False)
        with pytest.raises(Exception):
            job.validate()

        # Error not raised, py_job
        defaults_args['py_job'] = 'some_job.py'
        job = Job_Args_Parser(defaults_args=defaults_args, yml_args={}, job_args={}, cmd_args={}, build_yml_args=False, validate=False)
        try:
            job.validate()
        except Exception as exc:
            assert False, f"'test_validate_params' raised an exception: {exc}"

        # Error raised, sql_file
        defaults_args['py_job'] = 'sql_spark_job.py'
        job = Job_Args_Parser(defaults_args=defaults_args, yml_args={}, job_args={}, cmd_args={}, build_yml_args=False, validate=False)
        with pytest.raises(Exception):
            job.validate()

        # Error not raised, sql_file
        defaults_args['sql_file'] = 'some_job.sql'
        job = Job_Args_Parser(defaults_args=defaults_args, yml_args={}, job_args={}, cmd_args={}, build_yml_args=False, validate=False)
        try:
            job.validate()
        except Exception as exc:
            assert False, f"'test_validate_params' raised an exception: {exc}"

    def test_replace_placeholders(self):
        params = {
            'key1': ['I like {{key2}} pie', 'other_value'],
            'key2': 'some_value',
            'key3': {'other_key': 'a long string with {{key4}} in {{key5}}'},
            'key4': 'value_2',
            'key5': 'value_3'}
        actual = Job_Args_Parser.replace_placeholders(params)

        expected = {
            'key1': ['I like some_value pie', 'other_value'],
            'key2': 'some_value',
            'key3': {'other_key': 'a long string with value_2 in value_3'},
            'key4': 'value_2',
            'key5': 'value_3'}
        assert actual == expected

    def test_replace_placeholders_missing_cases(self):
        params = {
            'key1': ['I like {{key2}} pie', 'other_value'],
            'key2': 'some_value',
            'key3': {'other_key': 'a long string with {{key4}} in {{key5}} and {{key4}}'},
            'key4': 'value_2'}
        actual = Job_Args_Parser.replace_placeholders(params)

        expected = {
            'key1': ['I like some_value pie', 'other_value'],
            'key2': 'some_value',
            'key3': {'other_key': 'a long string with value_2 in {{key5}} and value_2'},
            'key4': 'value_2'}
        assert actual == expected


class Test_Runner(object):
    def test_run(self):
        from jobs.examples.ex7_pandas_job import Job
        job_args = {
            'job_param_file': None,
            'inputs': {
                'some_events': {'path': "./tests/fixtures/data_sample/wiki_example/input/", 'type': 'csv', 'df_type': 'pandas'},
                'other_events': {'path': "./tests/fixtures/data_sample/wiki_example/input/", 'type': 'csv', 'df_type': 'pandas'},
            },
            'output': {'path': 'n/a', 'type': 'None', 'df_type': 'pandas'},  # i.e. there is an output but it won't be dumped to disk.
            'spark_boot': False}
        job_post = Runner(Job, **job_args).run()  # will run the full job based on small scale data
        assert hasattr(job_post, 'out_df')

    def test_create_spark_submit_python_job(self):
        job_args = {
            'py_job': 'jobs/examples/ex7_pandas_job.py',
            'arg1': 'value1',
            'arg2': 'value2',
            'py-files': 'some/files.zip',
            'spark_submit_args': '--verbose',
            'spark_submit_keys': 'py-files',
            'spark_app_keys': 'arg1--arg2'}
        launch_jargs = Job_Args_Parser(defaults_args={}, yml_args={}, job_args=job_args, cmd_args={}, build_yml_args=False, loaded_inputs={})
        cmd_lst_real = Runner.create_spark_submit(jargs=launch_jargs)
        cmd_lst_expected = [
            'spark-submit',
            '--verbose',
            '--py-files=some/files.zip',
            'jobs/examples/ex7_pandas_job.py',
            '--arg1=value1',
            '--arg2=value2']
        assert cmd_lst_real == cmd_lst_expected

    def test_create_spark_submit_python_job_with_launcher(self):
        job_args = {
            'launcher_file': 'jobs/generic/launcher.py',
            'py_job': 'jobs/examples/ex7_pandas_job.py',
            'py-files': 'some/files.zip',
            'spark_submit_keys': 'py-files',
            'spark_app_keys': ''}
        launch_jargs = Job_Args_Parser(defaults_args={}, yml_args={}, job_args=job_args, cmd_args={}, build_yml_args=False, loaded_inputs={})
        cmd_lst_real = Runner.create_spark_submit(jargs=launch_jargs)
        cmd_lst_expected = [
            'spark-submit',
            '--py-files=some/files.zip',
            'jobs/examples/ex7_pandas_job.py']  # launcher.py not carried over. may want to change behavior.
        assert cmd_lst_real == cmd_lst_expected

    def test_create_spark_submit_jar_job(self):
        job_args = {
            'jar_job': 'jobs/examples/ex12_scala_job/target/spark_scala_job_2.13-1.0.jar',
            'spark_submit_args': '--verbose',
            'spark_app_args': 'jobs/examples/ex12_scala_job/some_text.txt'}
        launch_jargs = Job_Args_Parser(defaults_args={}, yml_args={}, job_args=job_args, cmd_args={}, build_yml_args=False, loaded_inputs={})
        cmd_lst_real = Runner.create_spark_submit(jargs=launch_jargs)
        cmd_lst_expected = [
            'spark-submit',
            '--verbose',
            'jobs/examples/ex12_scala_job/target/spark_scala_job_2.13-1.0.jar',
            'jobs/examples/ex12_scala_job/some_text.txt']
        assert cmd_lst_real == cmd_lst_expected


class Test_Flow(object):
    def test_create_connections_jobs(self, sc, sc_sql):
        cmd_args = {
            'deploy': 'none',
            'mode': 'dev_local',
            'job_param_file': JOBS_METADATA_FILE,
            'job_name': 'examples/ex4_dependency2_job.py',
            'storage': 'local',
        }
        launch_jargs = Job_Args_Parser(defaults_args={}, yml_args=None, job_args={}, cmd_args=cmd_args, build_yml_args=True, loaded_inputs={})
        connection_real = Flow.create_connections_jobs(launch_jargs.storage, launch_jargs.merged_args)
        connection_expected = pd.DataFrame(
            columns=['source_job', 'destination_job'],
            data=np.array([
                ['examples/ex0_extraction_job.py', 'examples/ex1_sql_job.sql'],
                ['examples/ex0_extraction_job.py', 'examples/ex7_pandas_job.py'],
                ['examples/ex0_extraction_job.py', 'examples/ex1_sql_spark_job'],
                ['examples/ex0_extraction_job.py', 'examples/ex1_frameworked_job.py'],
                ['examples/ex0_extraction_job.py', 'job_using_generic_template'],
                ['examples/ex3_incremental_prep_job.py', 'examples/ex3_incremental_job.py'],
                ['examples/ex4_dependency1_job.py', 'examples/ex4_dependency2_job.py'],
                ['examples/ex4_dependency2_job.py', 'examples/ex4_dependency3_job.sql'],
                ['examples/ex4_dependency1_job.py', 'examples/ex4_dependency3_job.sql'],
                ['examples/ex4_dependency3_job.sql', 'examples/ex4_dependency4_job.py'],
                ['examples/ex0_extraction_job.py', 'examples/run_jobs'],
                ['examples/ex1_sql_job.sql', 'examples/run_jobs'],
                ['examples/ex7_pandas_job.py', 'examples/run_jobs'],
                ['marketing/github_accounts_extraction_job.py', 'marketing/github_repos_extraction_job.py'],
                ['marketing/github_repos_extraction_job.py', 'marketing/github_contributors_extraction_job.py'],
                ['marketing/github_contributors_extraction_job.py', 'marketing/github_committers_extraction_job.py'],
                ['examples/ex1_sql_job.sql', 'dashboards/wikipedia_demo_dashboard.ipynb'],
                ['examples/ex7_pandas_job.py', 'dashboards/wikipedia_demo_dashboard.ipynb'],
            ]),
        )
        print(connection_real)
        assert_frame_equal(connection_real, connection_expected)

    def test_create_global_graph(self):
        import networkx as nx
        df = pd.DataFrame(
            columns=['source_job', 'destination_job'],
            data=np.array([
                ['examples/ex3_incremental_prep_job.py', 'examples/ex3_incremental_job.py'],
                ['examples/ex4_dependency1_job.py', 'examples/ex4_dependency2_job.py'],
                ['examples/ex4_dependency2_job.py', 'examples/ex4_dependency3_job.sql'],
                ['examples/ex4_dependency1_job.py', 'examples/ex4_dependency3_job.sql'],
                ['examples/ex4_dependency3_job.sql', 'examples/ex4_dependency4_job.py']]),
        )
        nx_real = Flow.create_global_graph(df)
        nx_expected = {
            'examples/ex3_incremental_prep_job.py': {'examples/ex3_incremental_job.py': {}},
            'examples/ex3_incremental_job.py': {},
            'examples/ex4_dependency1_job.py': {'examples/ex4_dependency2_job.py': {}, 'examples/ex4_dependency3_job.sql': {}},
            'examples/ex4_dependency2_job.py': {'examples/ex4_dependency3_job.sql': {}},
            'examples/ex4_dependency3_job.sql': {'examples/ex4_dependency4_job.py': {}},
            'examples/ex4_dependency4_job.py': {}
        }
        # Other way to check graph equality: nx.is_isomorphic(nx_real, nx_expected)
        assert nx.to_dict_of_dicts(nx_real) == nx_expected


def test_get_job_class():
    Job = get_job_class(py_job='jobs/examples/ex1_frameworked_job.py')  # must be real job
    assert issubclass(Job, ETL_Base)
