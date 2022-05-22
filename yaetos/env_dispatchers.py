"""
Set of operations that require dispatching between local and cloud environment.
"""
import boto3
from cloudpathlib import CloudPath
import io
from yaetos.pandas_utils import load_csvs, save_pandas_csv_local
from yaetos.logger import setup_logging
logger = setup_logging('Job')


class FS_Ops_Dispatcher2():
    # TODO: remove 'storage' var not used anymore accross all functions below, since now infered from path

    @staticmethod
    def is_s3_path(path):
        return path.startswith('s3://') or path.startswith('s3a://')

    @staticmethod
    def split_s3_path(fname):
        fname_parts = fname.split('s3://')[1].split('/')
        bucket_name = fname_parts[0]
        bucket_fname = '/'.join(fname_parts[1:])
        fname_parts = [item for item in fname_parts if item != '']
        return (bucket_name, bucket_fname, fname_parts)


    # --- load_pandas set of functions ----
    # TODO: deal with read_kwargs properly
    def load_pandas(self, fname, storage):
        return self.load_pandas_cluster(fname) if self.is_s3_path(fname) else self.load_pandas_local(fname)

    @staticmethod
    def load_pandas_local(fname):
        return load_csvs(fname, read_kwargs={})

    def load_pandas_cluster(self, fname):
        bucket_name, bucket_fname, fname_parts = self.split_s3_path(fname)
        local_path = 'tmp/s3_copy_'+fname_parts[-1]
        cp = CloudPath(fname)  # TODO: add way to load it with specific profile_name or client, as in "s3c = boto3.Session(profile_name='default').client('s3')"
        logger.info("Copying files from S3 '{}' to local '{}'. May take some time.".format(fname, local_path))
        local_pathlib = cp.download_to(local_path)
        local_path = local_path + '/' if local_pathlib.is_dir() else local_path
        logger.info("File copy finished")
        df = load_csvs(local_path, read_kwargs={})
        return df


    # --- save_pandas set of functions ----
    def save_pandas(self, df, fname, storage):
        return self.save_pandas_cluster(df, fname) if self.is_s3_path(fname) else self.save_pandas_local(df, fname)

    @staticmethod
    def save_pandas_local(df, fname):
        return save_pandas_csv_local(df, fname)

    def save_pandas_cluster(self, df, fname):
        # code below can be simplified using "df.to_csv(fname, **read_kwargs)", relying on s3fs library, but implies lots of dependencies, that break in cloud run.
        bucket_name, bucket_fname, fname_parts = self.split_s3_path(fname)
        read_kwargs={}  # TODO: integrate later.
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            s3c = boto3.Session(profile_name='default').client('s3')
            response = s3c.put_object(Bucket=bucket_name, Key=bucket_fname, Body=csv_buffer.getvalue())

        logger.info("Created file in S3: {}".format(fname))
        return df
