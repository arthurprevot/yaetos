"""
File needed to run jobs from the yaml manifest (jobs_metadata.yml)
Usage: $ python jobs/generic/launcher.py --job_name=some_job_name_from_manifest
"""

from yaetos.etl_utils import Commandliner

Commandliner(Job=None, launcher_file='jobs/generic/launcher.py')
