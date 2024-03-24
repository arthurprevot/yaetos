from yaetos.etl_utils import Commandliner
from yaetos.sql_spark_job import Job


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
