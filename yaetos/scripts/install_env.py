#!/usr/bin/env python
"""
Usage:
 * pip install yaetos
 * cd /path/to/an/empty/folder/that/will/contain/pipeline/code
 * yaetos setup  # -> will create sub-folders and setup required files.
 * yaetos launch_env  # -> To launch docker environment.

Alternative if last step doesn't work (due to unusual python or pip setup):
 * python -c 'from yaetos.scripts.install_env import YaetosCmds; YaetosCmds())'
"""

import os
from shutil import copyfile
import yaetos
import argparse
import sys

# TODO: replace duplicated files in yaetos/script folder to hard symlinks to avoid duplication.

class YaetosCmds(object):
    # Source: https://chase-seibert.github.io/blog/2014/03/21/python-multilevel-argparse.html

    usage_setup = "Setup yaetos folders and files in current folder."
    usage_launch_env = "Launching docker container to run jobs."

    usage = f'''
    yaetos <command> [<args>]

    Yaetos top level commands are:
    setup       {usage_setup}
    launch_env  {usage_launch_env}
    '''

    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Yeatos command lines',
            usage=self.usage)
        parser.add_argument('command', help='Subcommand to run')
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        getattr(self, args.command)() # dispatching according to command line.

    def setup(self):
        parser = argparse.ArgumentParser(
            description=self.usage_setup)
        parser.add_argument('--set_github', action='store_true')
        args = parser.parse_args(sys.argv[2:])  # ignoring first 2 args (i.e. "yeatos setup")
        setup_env(args)

    def launch_env(self):
        parser = argparse.ArgumentParser(
            description=self.usage_launch_env)
        # parser.add_argument('--no_aws', action='store_true')
        args = parser.parse_args(sys.argv[2:])  # ignoring first 2 args (i.e. "yeatos launch_env")
        launch_env()



def setup_env(args):
    cwd = os.getcwd()
    print(f'Will setup yaetos in the current folder ({cwd})')

    paths = yaetos.__path__
    package_path = paths[0]
    if len(paths) > 1 :
        print(f'Yeatos python package found in several locations. The script will use this one: {package_path}')

    # Empty folders necessary for later.
    os.system("mkdir -p tmp/files_to_ship/")
    os.system("mkdir -p data/")
    # TODO: make code above and below compatible with windows OS (no cmd line, no linux only paths).

    # Root folder files
    copyfile(f'{package_path}/scripts/copy/Dockerfile_external', f'{cwd}/Dockerfile')
    copyfile(f'{package_path}/scripts/copy/launch_env_external.sh', f'{cwd}/launch_env.sh')
    os.chmod(f'{cwd}/launch_env.sh', 0o755)  # TODO: use stat.S_IEXEC instead to be cross plateform

    # Conf
    os.system("mkdir -p conf/")
    copyfile(f'{package_path}/scripts/copy/aws_config.cfg.example', f'{cwd}/conf/aws_config.cfg')
    copyfile(f'{package_path}/scripts/copy/jobs_metadata_external.yml', f'{cwd}/conf/jobs_metadata.yml')
    copyfile(f'{package_path}/scripts/copy/connections.cfg.example', f'{cwd}/conf/connections.cfg')
    copyfile(f'{package_path}/scripts/copy/requirements_extra.txt', f'{cwd}/conf/requirements_extra.txt')

    # Sample jobs
    os.system("mkdir -p jobs/generic/")
    copyfile(f'{package_path}/libs/generic_jobs/copy_job.py', f'{cwd}/jobs/generic/copy_job.py')
    copyfile(f'{package_path}/libs/generic_jobs/deployer.py', f'{cwd}/jobs/generic/deployer.py')
    copyfile(f'{package_path}/libs/generic_jobs/dummy_job.py', f'{cwd}/jobs/generic/dummy_job.py')
    copyfile(f'{package_path}/libs/generic_jobs/launcher.py', f'{cwd}/jobs/generic/launcher.py')

    # Sample jobs
    os.system("mkdir -p jobs/examples/")
    copyfile(f'{package_path}/scripts/copy/ex0_extraction_job.py', f'{cwd}/jobs/examples/ex0_extraction_job.py')
    copyfile(f'{package_path}/scripts/copy/ex1_frameworked_job.py', f'{cwd}/jobs/examples/ex1_frameworked_job.py')
    copyfile(f'{package_path}/scripts/copy/ex1_full_sql_job.sql', f'{cwd}/jobs/examples/ex1_full_sql_job.sql')

    # Sample jobs tests
    os.system("mkdir -p tests/jobs/example/")
    copyfile(f'{package_path}/scripts/copy/conftest.py', f'{cwd}/tests/conftest.py')
    copyfile(f'{package_path}/scripts/copy/ex1_frameworked_job_test.py', f'{cwd}/tests/jobs/examples/ex1_frameworked_job_test.py')
    copyfile(f'{package_path}/scripts/copy/ex1_full_sql_job_test.py', f'{cwd}/tests/jobs/examples/ex1_full_sql_job_test.py')

    # TODO: add setup awscli or make sure it is there.

    # setup github CI
    if args.set_github:
        os.system("mkdir -p .github/workflows/")
        copyfile(f'{package_path}/scripts/github_pythonapp.yml', f'{cwd}/.github/workflows/pythonapp.yml')

    print('Done')


def launch_env():
    import subprocess
    subprocess.call("./launch_env.sh")
