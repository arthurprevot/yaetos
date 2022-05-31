from yaetos.etl_utils import ETLBase, Commandliner
from pyspark import sql


class Job(ETLBase):
    def transform(self, some_events: str = "some_events") -> sql.DataFrame:
        return self.query(
            f"""
            SELECT se.session_id, length(se.session_id) as session_length
            FROM {some_events} se
            """
        )


if __name__ == "__main__":
    args = {"job_param_file": "conf/jobs_metadata.yml"}
    Commandliner(Job, **args)
