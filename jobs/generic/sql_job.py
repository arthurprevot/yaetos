from yaetos.etl_utils import Commandliner
from yaetos.sql_job import Job as SqlJob

Commandliner(SqlJob)
# Commandliner(Job=None, launcher_file = 'jobs/generic/sql_job.py')

# works in local but not in EMR. TODO: check it, probably due to spark-submit options where sql_file was removed.

# ERROR msg:
# Traceback (most recent call last):
#   File "/home/hadoop/app/yaetos/sql_job.py", line 30, in <module>
#     Commandliner(Job)
#   File "/home/hadoop/app/scripts.zip/yaetos/etl_utils.py", line 908, in __init__
#     job = Job(pre_jargs={'defaults_args':defaults_args, 'job_args': job_args, 'cmd_args':cmd_args})  # can provide jargs directly here since job_file (and so job_name) needs to be extracted from job first. So, letting job build jargs.
#   File "/home/hadoop/app/scripts.zip/yaetos/etl_utils.py", line 69, in __init__
#     self.jargs = self.set_jargs(pre_jargs, loaded_inputs) if not jargs else jargs
#   File "/home/hadoop/app/yaetos/sql_job.py", line 9, in set_jargs
#     sql_file=pre_jargs['cmd_args']['sql_file']
# KeyError: 'sql_file'

# Also, need to check if it would work in pip installed setup.
