from yaetos.etl_utils import ETL_Base, Commandliner, get_aws_setup, FS_Ops_Dispatcher
from yaetos.etl_utils import Path_Handler
import os
from cloudpathlib import CloudPath as CPt
import fnmatch
import re


class Job(ETL_Base):
    def transform(self, files_to_copy):

        path_in = self.jargs.inputs['files_to_copy']['path']
        path_in = Path_Handler(path_in).expand_latest()
        regex=self.jargs.inputs['files_to_copy'].get('regex')
        globy=self.jargs.inputs['files_to_copy'].get('glob')

        self.logger.info(f"Path to scan = {path_in}, avec regex={regex} and glob={globy}")
        files_lst = FS_Ops_Dispatcher().list_files(path_in, regex=regex, globy=globy)

        path_out = self.jargs.output['path']
        path_out = Path_Handler(path_out).expand_now(self.start_dt)

        for file_in in files_lst:
            file_out = file_in.replace(path_in, path_out)
            FS_Ops_Dispatcher().copy_file(file_in, file_out) # need to enable path_raw_in in cloud and path_raw_out in local
            self.logger.info(f"Copied {file_in} to {file_out}.")

        self.logger.info("Finished copying all files")
        return None


        # path_raw_in = self.jargs.inputs['files_to_copy']['path']
        # # path_raw_in = self.expand_input_path(path_raw_in)
        # path_raw_in = CPt(path_raw_in)
        # self.logger.info(f"path_raw_in = {path_raw_in}")
        # path_raw_out = self.jargs.output['path']
        # # path_raw_out = self.expand_output_path(path_raw_out, now_dt=self.start_dt)
        # self.logger.info(f"path_raw_out = {path_raw_out}")

        # # Get pattern and pattern_type
        # if 'glob' in self.jargs.inputs['files_to_copy'].keys():
        #     pattern = self.jargs.inputs['files_to_copy']['glob']
        #     pattern_type = 'glob'
        # elif 'regex' in self.jargs.inputs['files_to_copy'].keys():
        #     pattern = self.jargs.inputs['files_to_copy']['regex']
        #     pattern_type = 'regex'
        # else:
        #     pattern = '*'
        #     pattern_type = 'glob'

        # # TODO: replace code below (and all functions) with the commented code
        # # FS_Ops_Dispatcher().copy_file(path_raw_in, path_raw_out) # need to enable path_raw_in in cloud and path_raw_out in local

        # session = get_aws_setup(self.jargs.merged_args)
        
        # s3 = session.client('s3')

        # if not CPt(f"s3://{path_raw_in.bucket}").exists() and self.jargs.merged_args.get('ignore_empty_bucket'):
        #     self.logger.warning("Bucket doesn't exit, or credentials not valid")
        #     return None

        # file_number = self.get_size(s3, path_raw_in.bucket, path_raw_in.key, pattern, pattern_type)
        # self.logger.info(f"Number of files to be downloaded {file_number}")

        # self.download_files(s3, path_raw_in.bucket, path_raw_in.key, pattern, pattern_type, path_raw_out)
        # self.logger.info("Finished downloading all files")
        # return None

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
