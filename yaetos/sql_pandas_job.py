from yaetos.etl_utils import Commandliner
from yaetos.sql_spark_job import Job as SQLJob


class Job(SQLJob):
    """To run/deploy sql jobs, using --sql_file arg, on top of pandas (using duckdb)"""
    def transform(self, **dfs):
        query_str = self.read_sql_file(self.jargs.sql_file)
        df = self.query(query_str, engine='pandas', dfs=dfs)
        return df


if __name__ == "__main__":
    Commandliner(Job)
