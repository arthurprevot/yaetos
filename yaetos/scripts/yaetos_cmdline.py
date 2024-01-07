#!/usr/bin/env python
"""
Script to setup commandline for yaetos when installed with pip install.

Usage:
 * pip install yaetos
 * cd /path/to/an/empty/folder/that/will/contain/pipeline/code
 * yaetos --help   # to get the options

For dev:
 * To use lib without publishing it: "cd path/to/repo/yeatos; pip install ." # TODO: check why changes are not always picked up.
 * Alternative if last step doesn't work (due to unusual python or pip setup): "python -c 'from yaetos.scripts.yaetos_cmdline import YaetosCmds; YaetosCmds()'"
 * This feeds into setup.py
"""

import os
import stat
from pathlib import Path as Pt
from shutil import copyfile
import yaetos
import argparse
import sys
import subprocess


class YaetosCmds(object):
    # Source: https://chase-seibert.github.io/blog/2014/03/21/python-multilevel-argparse.html

    usage_setup = "Setup yaetos folders and files in current folder. This will overwrite files if any."
    usage_docker_bash = "Launching docker container to run jobs from bash."
    usage_docker_jupyter = "Launching docker container to run jobs from jupyter notebook."
    usage_run_dockerized = "Run job through docker. Running 'yaetos run_dockerized some/job.py --some=arg' is the same as running 'yaetos usage_docker_bash; python some/job.py --some=arg'"
    usage_run = "Run job in terminal. Running 'yaetos run some/job.py --some=arg' is the same as running 'python some/job.py --some=arg'"
    usage_panel = "Run panel commandlines to create dashboards (experimental). Running 'yaetos panel some_args' is the same as running 'panel some_args'"

    usage = f'''
    yaetos <command> [<args>]

    Yaetos top level commands are:
    setup                : {usage_setup}
    launch_docker_bash   : {usage_docker_bash}
    launch_docker_jupyter: {usage_docker_jupyter}
    run_dockerized       : {usage_run_dockerized}
    run                  : {usage_run}
    panel                : {usage_panel}

    For more details, see:
    - https://yaetos.readthedocs.io/en/latest/
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
        getattr(self, args.command)()  # dispatching according to command line.

    def setup(self):
        parser = argparse.ArgumentParser(description=self.usage_setup)
        parser.add_argument('--set_github', action='store_true', help="Identify registry job to use.")
        parser.add_argument('--project_name', help="Set the name of the project, i.e. the name of the folder that will contain the job files.")
        args = parser.parse_args(sys.argv[2:])  # ignoring first 2 args (i.e. "yeatos setup")
        setup_env(args)

    def launch_docker_bash(self):
        argparse.ArgumentParser(description=self.usage_docker_bash)
        # parser.add_argument('--no_aws', action='store_true')  # TODO: implement
        out = subprocess.call("./launch_env.sh 1", shell=True)  # TODO: make it work with better: subprocess.call(["./launch_env.sh", '1']) # TODO: make it cross plateform (check using "sh launch_env.sh 1" for windows)
        self.print_error(out)

    def launch_docker_jupyter(self):
        argparse.ArgumentParser(description=self.usage_docker_jupyter)
        out = subprocess.call("./launch_env.sh 2", shell=True)
        self.print_error(out)

    def run_dockerized(self):
        parser = argparse.ArgumentParser(description=self.usage_run_dockerized)
        ignored, cmd_unknown_args = parser.parse_known_args()
        cmd_str = 'python ' + ' '.join(cmd_unknown_args[1:])
        cmd_delegated = "./launch_env.sh 3 " + cmd_str
        out = subprocess.call(cmd_delegated, shell=True)
        self.print_error(out)

    def run(self):
        parser = argparse.ArgumentParser(description=self.usage_run)
        ignored, cmd_unknown_args = parser.parse_known_args()
        cmd_str = 'python ' + ' '.join(cmd_unknown_args[1:])
        cmd_delegated = "./launch_env.sh 4 " + cmd_str
        out = subprocess.call(cmd_delegated, shell=True)
        self.print_error(out)

    def panel(self):
        parser = argparse.ArgumentParser(description=self.usage_panel)
        ignored, cmd_unknown_args = parser.parse_known_args()
        cmd_str = ' '.join(cmd_unknown_args[1:])
        cmd_delegated = "./launch_env.sh 4 " + cmd_str
        out = subprocess.call(cmd_delegated, shell=True)
        self.print_error(out)

    def print_error(self, out):
        if out == 127:  # 127 associated to error "/bin/sh: ./launch_env.sh: No such file or directory"
            print('Error: this command needs to be run from the project root, where launch_env.sh is, or have --path pointing to that folder.')


def main():
    # Necessary for setup.py entry_points to point to a function instead of a class (to avoid printing class repr in terminal)
    YaetosCmds()


def setup_env(args):
    cwd = os.getcwd()
    if args.project_name:
        os.makedirs(Pt(args.project_name), exist_ok=True)
        os.chdir(Pt(args.project_name))
        cwd = os.getcwd()
        print(f'Created the folder "{args.project_name}"')

    paths = yaetos.__path__
    package_path = paths[0]
    if len(paths) > 1:
        print(f'Yeatos python package found in several locations. The script will use this one: {package_path}')

    # Empty folders necessary for later.
    os.makedirs(Pt('tmp/files_to_ship/'), exist_ok=True)
    os.makedirs(Pt('data/'), exist_ok=True)
    # TODO: make code above and below compatible with windows OS (no cmd line, no linux only paths).

    # Root folder files
    copyfile(Pt(f'{package_path}/scripts/copy/Dockerfile_external'), Pt(f'{cwd}/Dockerfile'))
    copyfile(Pt(f'{package_path}/scripts/copy/launch_env_external.sh'), Pt(f'{cwd}/launch_env.sh'))
    copyfile(Pt(f'{package_path}/scripts/copy/_gitignore_external'), Pt(f'{cwd}/.gitignore'))
    os.chmod(Pt(f'{cwd}/launch_env.sh'), stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)  # equ to 0o755

    # Conf
    os.makedirs(Pt('conf/'), exist_ok=True)
    copyfile(Pt(f'{package_path}/scripts/copy/aws_config.cfg.example'), Pt(f'{cwd}/conf/aws_config.cfg'))
    copyfile(Pt(f'{package_path}/scripts/copy/jobs_metadata_external.yml'), Pt(f'{cwd}/conf/jobs_metadata.yml'))
    copyfile(Pt(f'{package_path}/scripts/copy/connections.cfg.example'), Pt(f'{cwd}/conf/connections.cfg'))
    copyfile(Pt(f'{package_path}/scripts/copy/requirements_extra.txt'), Pt(f'{cwd}/conf/requirements_extra.txt'))

    # Generic jobs
    os.makedirs(Pt('jobs/generic/'), exist_ok=True)
    copyfile(Pt(f'{package_path}/libs/generic_jobs/copy_job.py'), Pt(f'{cwd}/jobs/generic/copy_job.py'))
    copyfile(Pt(f'{package_path}/libs/generic_jobs/deployer.py'), Pt(f'{cwd}/jobs/generic/deployer.py'))
    copyfile(Pt(f'{package_path}/libs/generic_jobs/dummy_job.py'), Pt(f'{cwd}/jobs/generic/dummy_job.py'))
    copyfile(Pt(f'{package_path}/libs/generic_jobs/launcher.py'), Pt(f'{cwd}/jobs/generic/launcher.py'))
    copyfile(Pt(f'{package_path}/libs/generic_jobs/sql_spark_job.py'), Pt(f'{cwd}/jobs/generic/sql_spark_job.py'))
    copyfile(Pt(f'{package_path}/libs/generic_jobs/sql_pandas_job.py'), Pt(f'{cwd}/jobs/generic/sql_pandas_job.py'))

    # Sample jobs
    os.makedirs(Pt('jobs/examples/'), exist_ok=True)
    copyfile(Pt(f'{package_path}/scripts/copy/ex0_extraction_job.py'), Pt(f'{cwd}/jobs/examples/ex0_extraction_job.py'))
    copyfile(Pt(f'{package_path}/scripts/copy/ex1_pandas_api_job.py'), Pt(f'{cwd}/jobs/examples/ex1_pandas_api_job.py'))
    copyfile(Pt(f'{package_path}/scripts/copy/ex1_sql_job.sql'), Pt(f'{cwd}/jobs/examples/ex1_sql_job.sql'))
    copyfile(Pt(f'{package_path}/scripts/copy/ex1_spark_api_job.py'), Pt(f'{cwd}/jobs/examples/ex1_spark_api_job.py'))

    # Sample jobs tests
    os.makedirs(Pt('tests/jobs/examples/'), exist_ok=True)
    copyfile(Pt(f'{package_path}/scripts/copy/conftest.py'), Pt(f'{cwd}/tests/conftest.py'))
    copyfile(Pt(f'{package_path}/scripts/copy/ex1_pandas_api_job_test.py'), Pt(f'{cwd}/tests/jobs/examples/ex1_pandas_api_job_test.py'))
    copyfile(Pt(f'{package_path}/scripts/copy/ex1_spark_api_job_test.py'), Pt(f'{cwd}/tests/jobs/examples/ex1_spark_api_job_test.py'))
    copyfile(Pt(f'{package_path}/scripts/copy/ex1_sql_pandas_job_test.py'), Pt(f'{cwd}/tests/jobs/examples/ex1_sql_pandas_job_test.py'))
    copyfile(Pt(f'{package_path}/scripts/copy/ex1_sql_spark_job_test.py'), Pt(f'{cwd}/tests/jobs/examples/ex1_sql_spark_job_test.py'))

    # TODO: add setup awscli or make sure it is there.

    # setup github CI
    if args.set_github:
        os.makedirs(Pt('.github/workflows/'), exist_ok=True)
        copyfile(Pt(f'{package_path}/scripts/github_pythonapp.yml'), Pt(f'{cwd}/.github/workflows/pythonapp.yml'))

    print("""Filled the folder with:
     * sample jobs, ready for local execution,
     * a docker environment setup with spark,
     * a configuration file for cloud execution,
     * job unit-tests.""")
