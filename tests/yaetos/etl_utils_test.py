import pytest
from pandas.testing import assert_frame_equal
import pandas as pd
import numpy as np
from yaetos.etl_utils import (
    ETLBase,
    Commandliner,
    Period_Builder,
    Job_Args_Parser,
    Job_Yml_Parser,
    Flow,
    get_job_class,
    LOCAL_APP_FOLDER,
    JOBS_METADATA_FILE,
)


class TestETLBase:
    def test_check_pk(self, sc, sc_sql, ss):
        sdf = ss.read.json(sc.parallelize([{"id": 1}, {"id": 2}, {"id": 3}]))

        pks = ["id"]
        assert ETLBase.check_pk(sdf, pks) is True

        sdf = ss.read.json(sc.parallelize([{"id": 1}, {"id": 2}, {"id": 2}]))

        pks = ["id"]
        assert ETLBase.check_pk(sdf, pks) is False

    # def test_set_py_job(self, get_pre_jargs):  # works locally but not from CI tool, where LOCAL_APP_FOLDER is different.
    #     py_job = ETLBase(pre_jargs=get_pre_jargs({})).set_py_job()
    #     assert py_job == LOCAL_APP_FOLDER+'yaetos/etl_utils.py' # file is the one that starts execution, typically the job python file.

    def test_load_inputs(self, sc, sc_sql, ss, get_pre_jargs):
        """Confirming load_inputs acts as a passthrough"""
        sdf = ss.read.json(sc.parallelize([{"id": 1}, {"id": 2}, {"id": 3}]))
        loaded_inputs = {"input1": sdf}
        app_args_expected = loaded_inputs
        assert (
            ETLBase(pre_jargs=get_pre_jargs(loaded_inputs.keys())).load_inputs(
                loaded_inputs
            )
            == app_args_expected
        )

    def test_get_max_timestamp(self, sc, sc_sql, ss, get_pre_jargs):
        sdf = ss.read.json(
            sc.parallelize(
                [
                    {"id": 1, "timestamp": "2020-01-01"},
                    {"id": 2, "timestamp": "2020-01-02"},
                    {"id": 3, "timestamp": "2020-01-03"},
                ]
            )
        )
        pre_jargs_over = {
            "defaults_args": {
                "inputs": {},
                "output": {"inc_field": "timestamp", "type": None},
            }
        }
        max_timestamp_expected = "2020-01-03"
        assert (
            ETLBase(
                pre_jargs=get_pre_jargs(pre_jargs_over=pre_jargs_over)
            ).get_max_timestamp(sdf)
            == max_timestamp_expected
        )


class TestPeriodBuilder:
    def test_get_last_day(self):
        from datetime import datetime

        as_of_date = datetime.strptime("2021-01-02", "%Y-%m-%d")
        last_day_real = Period_Builder.get_last_day(as_of_date)
        last_day_expected = "2021-01-01"
        assert last_day_real == last_day_expected

    def test_get_first_to_last_day(self):
        from datetime import datetime

        first_day = "2021-01-01"
        as_of_date = datetime.strptime("2021-01-05", "%Y-%m-%d")
        period_real = Period_Builder.get_first_to_last_day(first_day, as_of_date)
        period_expected = ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04"]
        assert period_real == period_expected

    def test_get_last_output_to_last_day(self):
        from datetime import datetime

        first_day_input = "2021-01-01"
        last_run_period = "2021-01-03"
        as_of_date = datetime.strptime("2021-01-08", "%Y-%m-%d")
        period_real = Period_Builder().get_last_output_to_last_day(
            last_run_period, first_day_input, as_of_date
        )
        period_expected = ["2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07"]
        assert period_real == period_expected


class TestJobYmlParser:
    def test_set_py_job_from_name(self):
        py_job = Job_Yml_Parser.set_py_job_from_name("some_job_name")
        assert py_job == "jobs/some_job_name"

    def test_set_job_name_from_file(self):
        job_name = Job_Yml_Parser.set_job_name_from_file("jobs/some/file.py")
        assert job_name == "some/file.py"

        job_name = Job_Yml_Parser.set_job_name_from_file(
            LOCAL_APP_FOLDER + "jobs/some/file.py"
        )
        assert job_name == "some/file.py"

    # def test_set_sql_file_from_name(self) # to be added
    #     Job_Yml_Parser.set_sql_file_from_name()


class TestJob_Args_Parser:
    def test_no_param_override(self):
        defaults_args = {
            "mode": "dev_local",
            "deploy": "code",
            "output": {"path": "n/a", "type": "csv"},
        }
        expected_args = {**{"inputs": {}, "is_incremental": False}, **defaults_args}

        jargs = Job_Args_Parser(
            defaults_args=defaults_args, yml_args={}, job_args={}, cmd_args={}
        )
        assert jargs.merged_args == expected_args


class TestFlow:
    def test_create_connections_jobs(self, sc, sc_sql):
        cmd_args = {
            "deploy": "none",
            "mode": "dev_local",
            "job_param_file": JOBS_METADATA_FILE,
            "job_name": "examples/ex4_dependency2_job.py",
            "storage": "local",
        }
        launch_jargs = Job_Args_Parser(
            defaults_args={},
            yml_args=None,
            job_args={},
            cmd_args=cmd_args,
            loaded_inputs={},
        )
        connection_real = Flow.create_connections_jobs(
            launch_jargs.storage, launch_jargs.merged_args
        )
        connection_expected = pd.DataFrame(
            columns=["source_job", "destination_job"],
            data=np.array(
                [
                    ["examples/ex0_extraction_job.py", "examples/ex1_full_sql_job.sql"],
                    [
                        "examples/ex0_extraction_job.py",
                        "examples/ex1_frameworked_job.py",
                    ],
                    ["examples/ex0_extraction_job.py", "job_using_generic_template"],
                    [
                        "examples/ex3_incremental_prep_job.py",
                        "examples/ex3_incremental_job.py",
                    ],
                    [
                        "examples/ex4_dependency1_job.py",
                        "examples/ex4_dependency2_job.py",
                    ],
                    [
                        "examples/ex4_dependency2_job.py",
                        "examples/ex4_dependency3_job.sql",
                    ],
                    [
                        "examples/ex4_dependency1_job.py",
                        "examples/ex4_dependency3_job.sql",
                    ],
                    [
                        "examples/ex4_dependency3_job.sql",
                        "examples/ex4_dependency4_job.py",
                    ],
                    [
                        "examples/ex0_extraction_job.py",
                        "examples/ex7_extraction_small_job.py",
                    ],
                ]
            ),
        )
        assert_frame_equal(connection_real, connection_expected)

    def test_create_global_graph(self):
        import networkx as nx

        df = pd.DataFrame(
            columns=["source_job", "destination_job"],
            data=np.array(
                [
                    [
                        "examples/ex3_incremental_prep_job.py",
                        "examples/ex3_incremental_job.py",
                    ],
                    [
                        "examples/ex4_dependency1_job.py",
                        "examples/ex4_dependency2_job.py",
                    ],
                    [
                        "examples/ex4_dependency2_job.py",
                        "examples/ex4_dependency3_job.sql",
                    ],
                    [
                        "examples/ex4_dependency1_job.py",
                        "examples/ex4_dependency3_job.sql",
                    ],
                    [
                        "examples/ex4_dependency3_job.sql",
                        "examples/ex4_dependency4_job.py",
                    ],
                ]
            ),
        )
        nx_real = Flow.create_global_graph(df)
        nx_expected = {
            "examples/ex3_incremental_prep_job.py": {
                "examples/ex3_incremental_job.py": {}
            },
            "examples/ex3_incremental_job.py": {},
            "examples/ex4_dependency1_job.py": {
                "examples/ex4_dependency2_job.py": {},
                "examples/ex4_dependency3_job.sql": {},
            },
            "examples/ex4_dependency2_job.py": {"examples/ex4_dependency3_job.sql": {}},
            "examples/ex4_dependency3_job.sql": {"examples/ex4_dependency4_job.py": {}},
            "examples/ex4_dependency4_job.py": {},
        }
        # Other way to check graph equality: nx.is_isomorphic(nx_real, nx_expected)
        assert nx.to_dict_of_dicts(nx_real) == nx_expected


def test_get_job_class():
    Job = get_job_class(
        py_job="jobs/examples/ex1_frameworked_job.py"
    )  # must be real job
    assert issubclass(Job, ETLBase)
