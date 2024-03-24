from yaetos.etl_utils import ETL_Base, Commandliner, get_aws_setup
import os
from cloudpathlib import CloudPath as CPt
import fnmatch
import re


class Job(ETL_Base):
    def transform(self, files_to_copy):
        path_raw_in = self.jargs.inputs['files_to_copy']['path']
        path_raw_in = self.expand_input_path(path_raw_in)
        path_raw_in = CPt(path_raw_in)
        path_raw_out = self.jargs.output['path']
        path_raw_out = self.expand_output_path(path_raw_out, now_dt=self.start_dt)

        # Get pattern and pattern_type
        if 'glob' in self.jargs.inputs['files_to_copy'].keys():
            pattern = self.jargs.inputs['files_to_copy']['glob']
            pattern_type = 'glob'
        elif 'regex' in self.jargs.inputs['files_to_copy'].keys():
            pattern = self.jargs.inputs['files_to_copy']['regex']
            pattern_type = 'regex'
        else:
            pattern = '*'
            pattern_type = 'glob'

        session = get_aws_setup(self.jargs.merged_args)
        s3 = session.client('s3')

        file_number = self.get_size(s3, path_raw_in.bucket, path_raw_in.key, pattern, pattern_type)
        self.logger.info(f"Number of files to be downloaded {file_number}")

        self.download_files(s3, path_raw_in.bucket, path_raw_in.key, pattern, pattern_type, path_raw_out)
        self.logger.info("Finished downloading all files")
        return None

    def download_files(self, s3, bucket_name, prefix, pattern, pattern_type, path_raw_out):
        # Create the local directory if it doesn't exist
        if not os.path.exists(path_raw_out):
            os.makedirs(path_raw_out)

        for (obj, file_name) in self.s3_iterator(s3, bucket_name, prefix, pattern, pattern_type):
            local_file_path = os.path.join(path_raw_out, file_name)

            # Create subdirectories if they don't exist
            local_file_directory = os.path.dirname(local_file_path)
            if not os.path.exists(local_file_directory):
                os.makedirs(local_file_directory)

            # Download the file
            s3.download_file(bucket_name, obj['Key'], local_file_path)
            print(f"Downloaded {obj['Key']} to {local_file_path}")

    def get_size(self, s3, bucket_name, prefix, pattern, pattern_type):
        matching_files_count = 0
        for (obj, file_name) in self.s3_iterator(s3, bucket_name, prefix, pattern, pattern_type):
            matching_files_count += 1
        return matching_files_count

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
