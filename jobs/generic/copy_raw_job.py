from yaetos.etl_utils import ETL_Base, Commandliner
import boto3
import os
from pathlib import Path as Pt
from cloudpathlib import CloudPath as CPt
import fnmatch
import re

class Job(ETL_Base):

    def transform(self, table_to_copy):

        # Paths
        path_raw_in = self.jargs.inputs['table_to_copy']['path']
        path_raw_in2 = self.expand_input_path(path_raw_in)
        path_raw_in3 = CPt(path_raw_in2)
        path_raw_out = self.jargs.output['path']
        path_raw_out2 = self.expand_output_path(path_raw_out, now_dt=self.start_dt)
        # globy = self.jargs.inputs['table_to_copy'].get('glob', '*')
        # regy = self.jargs.inputs['table_to_copy'].get('regex', '*')
        if 'glob' in self.jargs.inputs['table_to_copy'].keys():
            pattern = self.jargs.inputs['table_to_copy']['glob']
            pattern_type = 'glob'
        elif 'regex' in self.jargs.inputs['table_to_copy'].keys():
            pattern = self.jargs.inputs['table_to_copy']['regex']
            pattern_type = 'regex'
        else:
            pattern = '*'
            pattern_type = 'glob'

        # Initialize a boto3 session
        session = boto3.session.Session()
        s3 = session.client('s3')

        bucket_name = path_raw_in3.bucket  # Replace with your bucket name
        prefix = path_raw_in3.key  # Replace with the folder path in your bucket
        local_directory = path_raw_out2  # Replace with your local directory path
        # pattern =   # Replace with your glob pattern, e.g., '*.txt' for all text files

        # import ipdb; ipdb.set_trace()
        file_number = self.get_size(s3, bucket_name, prefix, pattern, pattern_type)
        self.logger.info(f"Number of files to be downloaded {file_number}")

        # Create the local directory if it doesn't exist
        if not os.path.exists(local_directory):
            os.makedirs(local_directory)

        # List objects within the specified folder
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_name = obj['Key'][len(prefix):]
                    if pattern_type == 'glob':
                        match = fnmatch.fnmatch(file_name, pattern)
                    elif pattern_type == 'regex':
                        match = re.match(pattern, file_name)
                    else:
                        match = True

                    if match:
                        # Extract the file name from the object key and create its local path
                        local_file_path = os.path.join(local_directory, file_name)

                        # Create subdirectories if they don't exist
                        local_file_directory = os.path.dirname(local_file_path)
                        if not os.path.exists(local_file_directory):
                            os.makedirs(local_file_directory)

                        # Download the file
                        s3.download_file(bucket_name, obj['Key'], local_file_path)
                        print(f"Downloaded {obj['Key']} to {local_file_path}")

        return None

    def get_size(self, s3, bucket_name, prefix, pattern, pattern_type):
        matching_files_count = 0

        # List objects within the specified folder
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_name = obj['Key'][len(prefix):]
                    if pattern_type == 'glob':
                        match = fnmatch.fnmatch(file_name, pattern)
                    elif pattern_type == 'regex':
                        match = re.match(pattern, file_name)
                    else:
                        match = True

                    if match:
                        # Increment the counter for each matching file
                        matching_files_count += 1
        return matching_files_count


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
