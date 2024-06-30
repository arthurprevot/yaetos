import yaml
import os
import subprocess
from yaetos.logger import setup_logging
logger = setup_logging('Job')


class Git_Config_Manager():

    FNAME = 'conf/git_config.yml'

    def get_config(self, mode, **kwargs):

        # Deal with multiple modes if any (Hacky. TODO: improve)
        modes = mode.split(',')
        if 'dev_local' in modes:
            config = self.get_config_from_git(kwargs['local_app_folder'])
            # For debug: self.save_yaml(config)
        elif 'dev_EMR' in modes or 'prod_EMR' in modes:  # TODO: remove dependency on 'dev_EMR' and 'prod_EMR'.
            config = self.get_config_from_file(kwargs['cluster_app_folder'])
        else:
            required_mode = ('dev_local', 'dev_EMR', 'prod_EMR')  # TODO: same
            raise Exception(f'Wrong mode, one of the mode should be in {required_mode}')
        return config

    def get_config_from_git(self, local_app_folder):

        # Current directory, i.e. yeatos job folder
        branch = subprocess.check_output(["git", "describe", '--all']).strip().decode('ascii')  # to get if dirty, add '--dirty'.
        last_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip().decode('ascii')
        diffs = subprocess.check_output(['git', 'diff', 'HEAD']).strip().decode('ascii')
        is_dirty = True if diffs else False

        # Yaetos directory, i.e. framework folder. TODO: check how to handle when framework code is pulled from pip installed lib.
        branch_yaetos = subprocess.check_output(['git', 'describe', '--all'], cwd=local_app_folder).strip().decode('ascii')
        last_commit_yaetos = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=local_app_folder).strip().decode('ascii')
        diffs_yaetos = subprocess.check_output(['git', 'diff', 'HEAD'], cwd=local_app_folder).strip().decode('ascii')
        is_dirty_yaetos = True if diffs_yaetos else False

        config = {'branch_current': branch,
                  'last_commit_current': last_commit,
                  'diffs_current': diffs,
                  'is_dirty_current': is_dirty,
                  'branch_yaetos': branch_yaetos,
                  'last_commit_yaetos': last_commit_yaetos,
                  'diffs_yaetos': diffs_yaetos,
                  'is_dirty_yaetos': is_dirty_yaetos
                  }
        return config

    def is_git_controlled(self):
        out = os.system('git rev-parse')  # not using subprocess.check_output() to avoid crash if it fails.
        if out == 0:
            return True
        else:
            return False  # will send "fatal: not a git repository" or "git: command not found" to stderr

    def save_yaml(self, config):
        os.makedirs(os.path.dirname(self.FNAME), exist_ok=True)
        with open(self.FNAME, 'w') as file:
            yaml.dump(config, file)
        logger.info('Saved yml with git info: {}'.format(self.FNAME))

    def get_config_from_file(self, cluster_app_folder):
        """Meant to work in EMR"""
        fname = cluster_app_folder + self.FNAME
        if os.path.isfile(fname):
            with open(fname, 'r') as stream:
                yml = yaml.load(stream, Loader=yaml.FullLoader)
            return yml
        else:
            return False
