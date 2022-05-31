import pytest
from jobs.examples.ex4_dependency1_job import Job


class TestJob:
    def test_transform(self, sc, sc_sql, ss, get_pre_jargs):
        some_events = ss.read.json(
            sc.parallelize(
                [
                    {"session_id": 1},
                    {"session_id": 12},
                    {"session_id": 123},
                    {"session_id": 1234},
                ]
            )
        )

        expected = [
            {"session_id": 1, "session_length": 1},
            {"session_id": 12, "session_length": 2},
            {"session_id": 123, "session_length": 3},
            {"session_id": 1234, "session_length": 4},
        ]

        loaded_inputs = {"some_events": some_events}
        actual = (
            Job(pre_jargs=get_pre_jargs(loaded_inputs.keys()))
            .etl_no_io(sc, sc_sql, loaded_inputs=loaded_inputs)[0]
            .toPandas()
            .to_dict(orient="records")
        )
        assert actual == expected
