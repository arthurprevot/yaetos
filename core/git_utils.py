import yaml
import os
import subprocess


class Git_Config_Manager():

    FNAME = 'conf/git_config.yml'

    def get_config(self, local_app_folder):
        if self.is_git_controlled():
            config = self.get_config_from_git(local_app_folder)
            self.save_yaml(config)
        else:
            config = self.get_config_from_file()
        return config

    def get_config_from_git(self, local_app_folder):

        branch = subprocess.check_output(["git", "describe", '--all']).strip().decode('ascii')  # to get if dirty, add '--dirty'.
        last_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip().decode('ascii')
        diffs = subprocess.check_output(['git', 'diff', 'HEAD']).strip().decode('ascii')
        is_dirty = True if diffs else False
        branch_yaetos = subprocess.check_output(['git', 'describe', '--all'], cwd=local_app_folder).strip().decode('ascii')
        last_commit_yaetos = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=local_app_folder).strip().decode('ascii')
        diffs_yaetos = subprocess.check_output(['git', 'diff', 'HEAD'], cwd=local_app_folder).strip().decode('ascii')
        is_dirty_yaetos = True if diffs_yaetos else False

        config = {'branch':branch,
                  'last_commit':last_commit,
                  'diffs':diffs,
                  'is_dirty':is_dirty,
                  'branch_yaetos':branch_yaetos,
                  'last_commit_yaetos':last_commit_yaetos,
                  'diffs_yaetos':diffs_yaetos,
                  'is_dirty_yaetos':is_dirty_yaetos
                  }
        return config

    def is_git_controlled(self):
        import subprocess
        out = os.system('git rev-parse')  # not using subprocess.check_output() to avoid crash if it fails.
        if out == 0:
            return True
        else:
            return False  # will send "fatal: not a git repository" to stderr

    def save_yaml(self, config):
        os.makedirs(os.path.dirname(self.FNAME), exist_ok=True)
        with open(self.FNAME, 'w') as file:
            documents = yaml.dump(config, file)

    def get_config_from_file():
        if os.path.isfile(self.FNAME):
            return Job_Yml_Parser.load_meta(self.FNAME)
        else:
            return False
