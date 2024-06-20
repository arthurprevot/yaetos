from yaetos.etl_utils import ETL_Base, Commandliner, get_aws_setup
from cloudpathlib import CloudPath as CPt
import fnmatch
import re
import pandas as pd


class Job(ETL_Base):
    def transform(self, files):
        path_raw_in = self.jargs.inputs['files']['path']
        path_raw_in = self.expand_input_path(path_raw_in)
        path_raw_in = CPt(path_raw_in)
        self.logger.info(f"path_raw_in = {path_raw_in}")
        path_raw_out = self.jargs.output['path']
        path_raw_out = self.expand_output_path(path_raw_out, now_dt=self.start_dt)
        self.logger.info(f"path_raw_out = {path_raw_out}")

        # Get pattern and pattern_type
        if 'glob' in self.jargs.inputs['files'].keys():
            pattern = self.jargs.inputs['files']['glob']
            pattern_type = 'glob'
        elif 'regex' in self.jargs.inputs['files'].keys():
            pattern = self.jargs.inputs['files']['regex']
            pattern_type = 'regex'
        else:
            pattern = '*'
            pattern_type = 'glob'

        session = get_aws_setup(self.jargs.merged_args)

        s3 = session.client('s3')

        if not CPt(f"s3://{path_raw_in.bucket}").exists() and self.jargs.merged_args.get('ignore_empty_bucket'):
            self.logger.warning("Bucket doesn't exit, or credentials not valid")
            return None

        files = self.get_filenames(s3, path_raw_in.bucket, path_raw_in.key, pattern, pattern_type)
        files_df = pd.DataFrame(files, columns=['filenames'])
        self.logger.info(f"Number of files to be downloaded {len(files)}")
        return files_df

    def get_filenames(self, s3, bucket_name, prefix, pattern, pattern_type):
        files = []
        for (obj, file_name) in self.s3_iterator(s3, bucket_name, prefix, pattern, pattern_type):
            files.append('s3://' + bucket_name + '/' + obj['Key'])
        return files

    def s3_iterator(self, s3, bucket_name, prefix, pattern, pattern_type):
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_name = obj['Key'][len(prefix):]
                    match = self.get_match(file_name, pattern, pattern_type)
                    if match:
                        yield obj, file_name

    @staticmethod
    def get_match(file_name, pattern, pattern_type):
        if pattern_type == 'glob':
            match = fnmatch.fnmatch(file_name, pattern)
        elif pattern_type == 'regex':
            match = re.match(pattern, file_name)
        else:
            match = True
        return match


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
