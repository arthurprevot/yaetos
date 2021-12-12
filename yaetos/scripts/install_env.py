"""
Usage:
 * pip install yaetos
 * cd /path/to/an/empty/folder/that/will/contain/pipeline/code
 * yaetos_install  # -> will create folders and copy files.

Alternative if last step doesn't work (due to unusual python or pip setup):
 * python -c 'from yaetos.scripts.install_env import main; main())'
"""
import os
from shutil import copyfile
import yaetos

def main():
    cwd = os.getcwd()
    print(f'Starting yaetos setup in the current folder ({cwd})')

    paths = yaetos.__path__
    if len(paths) > 1 :
        print('Yeatos python package found in several locations. The script will choose one.')
    package_path = paths[0]
    print(f'Yeatos python package path to be used to pull setup information: {package_path}')

    # Empty folders necessary for later.
    os.system("mkdir -p tmp/files_to_ship/")
    os.system("mkdir -p data/")

    # Root folder files
    copyfile(f'{package_path}/scripts/Dockerfile_external', f'{cwd}/Dockerfile')
    copyfile(f'{package_path}/scripts/launch_env_external.sh', f'{cwd}/launch_env.sh')

    # Conf
    os.system("mkdir -p conf/")
    copyfile(f'{package_path}/scripts/aws_config.cfg.example', f'{cwd}/conf/aws_config.cfg')
    copyfile(f'{package_path}/scripts/jobs_metadata_external.yml', f'{cwd}/conf/jobs_metadata.yml')
    copyfile(f'{package_path}/scripts/connections.cfg.example', f'{cwd}/conf/connections.cfg')

    # Sample jobs
    os.system("mkdir -p jobs/examples/")
    copyfile(f'{package_path}/scripts/ex0_extraction_job.py', f'{cwd}/jobs/examples/ex0_extraction_job.py')
    copyfile(f'{package_path}/scripts/ex1_frameworked_job.py', f'{cwd}/jobs/examples/ex1_frameworked_job.py')
    copyfile(f'{package_path}/scripts/ex1_full_sql_job.sql', f'{cwd}/jobs/examples/ex1_full_sql_job.sql')

    # Sample jobs tests
    os.system("mkdir -p tests/jobs/example/")
    copyfile(f'{package_path}/scripts/conftest.py', f'{cwd}/tests/conftest.py')
    copyfile(f'{package_path}/scripts/ex1_frameworked_job_test.py', f'{cwd}/tests/jobs/example/ex1_frameworked_job_test.py')
    copyfile(f'{package_path}/scripts/ex1_full_sql_job_test.py', f'{cwd}/tests/jobs/example/ex1_full_sql_job_test.py')

    # setup awscli or make sure it is there.
    # run = True
    # if run:
    #     check aws

    # setup github CI
    run = False
    if run:
        os.system("mkdir -p .github/workflows/")
        # copyfile(f'{package_path}/.github/workflows/pythonapp.yml', f'{cwd}/.github/workflows/')

    # import sys
    # sys.exit()
