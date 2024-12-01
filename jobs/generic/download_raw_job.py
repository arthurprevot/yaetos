"""
Meant to download data from S3 to local. Meant to work locally only, i.e. not from cloud.
"""
from yaetos.etl_utils import ETL_Base, Commandliner, FS_Ops_Dispatcher, Path_Handler
import boto3
from pathlib import Path
from cloudpathlib import S3Path
import os


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
            self.download_file_cluster(file_in, file_out) # TODO: enable path_raw_in in cloud and path_raw_out in local
            self.logger.info(f"Copied {file_in} to {file_out}.")

        self.logger.info("Finished copying all files")
        return None

    @staticmethod
    def download_file_cluster(path_in, path_out):
        s3 = boto3.client('s3')

        path_in_obj = S3Path(path_in)
        bucket_in = path_in_obj.bucket
        key_in = path_in_obj.key

        path_out_obj = Path(path_out)
        path_out_folder = str(path_out_obj.parent)
        os.makedirs(path_out_folder, exist_ok=True)

        s3.download_file(bucket_in, key_in, path_out)


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
