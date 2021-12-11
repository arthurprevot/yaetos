"""
Usage:
 * pip install yaetos
 * cd /path/to/an/empty/folder/
 * python -c 'import yaetos; yaetos.scripts.install_env)'
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


    os.system("mkdir -p tmp/files_to_ship/")
    os.system("mkdir -p data/")

    # copyfile(f'{package_path}/scripts/Dockerfile', cwd)
    # copyfile(f'{package_path}/scripts/launch_env_external.sh', f'{cwd}/scripts/launch_env.sh')

    os.system("mkdir -p conf/")
    # copyfile(f'{package_path}/conf/aws_config.cfg.example', f'{cwd}/conf/aws_config.cfg')
    # copyfile(f'{package_path}/conf/jobs_metadata.yml', f'{cwd}/conf/')

    os.system("mkdir -p jobs/")


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
