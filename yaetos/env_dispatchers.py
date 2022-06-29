"""
Set of operations that require dispatching between local and cloud environment.
"""
import boto3
import os
from time import sleep
from io import StringIO
# from sklearn.externals import joblib  # TODO: re-enable after fixing lib versions.
from configparser import ConfigParser
from yaetos.pandas_utils import load_df, save_pandas_local
from yaetos.logger import setup_logging
logger = setup_logging('Job')


class FS_Ops_Dispatcher():
    """Set of functions to dispatch mostly IO methods to local or cloud depending on the path being local or cloud (s3://*)."""

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

    # --- save_metadata set of functions ----

    def save_metadata(self, fname, content):
        self.save_metadata_cluster(fname, content) if self.is_s3_path(fname) else self.save_metadata_local(fname, content)

    @staticmethod
    def save_metadata_local(fname, content):
        fh = open(fname, 'w')
        fh.write(content)
        fh.close()
        logger.info("Created file locally: {}".format(fname))

    @staticmethod
    def save_metadata_cluster(fname, content):
        fname_parts = fname.split('s3://')[1].split('/')
        bucket_name = fname_parts[0]
        bucket_fname = '/'.join(fname_parts[1:])
        fake_handle = StringIO(content)
        s3c = boto3.Session(profile_name='default').client('s3')
        s3c.put_object(Bucket=bucket_name, Key=bucket_fname, Body=fake_handle.read())
        logger.info("Created file S3: {}".format(fname))

    # --- save_file set of functions ----
    # Disabled until joblib enabled. Will be useful for ML use case.
    # def save_file(self, fname, content):
    #     self.save_file_cluster(fname, content) if self.is_s3_path(fname) else self.save_file_local(fname, content)
    #
    # @staticmethod
    # def save_file_local(fname, content):
    #     folder = os.path.dirname(fname)
    #     if not os.path.exists(folder):
    #         os.makedirs(folder)
    #     joblib.dump(content, fname)
    #     logger.info("Saved content to new file locally: {}".format(fname))
    #
    # def save_file_cluster(self, fname, content):
    #     fname_parts = fname.split('s3://')[1].split('/')
    #     bucket_name = fname_parts[0]
    #     bucket_fname = '/'.join(fname_parts[1:])
    #     s3c = boto3.Session(profile_name='default').client('s3')
    #
    #     # local_path = CLUSTER_APP_FOLDER+'tmp/local_'+fname_parts[-1]
    #     local_path = 'tmp/local_'+fname_parts[-1]
    #     self.save_file_local(local_path, content)
    #     fh = open(local_path, 'rb')
    #     s3c.put_object(Bucket=bucket_name, Key=bucket_fname, Body=fh)
    #     logger.info("Pushed local file to S3, from '{}' to '{}' ".format(local_path, fname))
    #
    # # --- load_file set of functions ----
    # def load_file(self, fname):
    #     return self.load_file_cluster(fname) if self.is_s3_path(fname) else self.load_file_local(fname)
    #
    # @staticmethod
    # def load_file_local(fname):
    #     return joblib.load(fname)
    #
    # @staticmethod
    # def load_file_cluster(fname):
    #     fname_parts = fname.split('s3://')[1].split('/')
    #     bucket_name = fname_parts[0]
    #     bucket_fname = '/'.join(fname_parts[1:])
    #     # local_path = CLUSTER_APP_FOLDER+'tmp/s3_'+fname_parts[-1]
    #     local_path = 'tmp/s3_'+fname_parts[-1]
    #     s3c = boto3.Session(profile_name='default').client('s3')
    #     s3c.download_file(bucket_name, bucket_fname, local_path)
    #     logger.info("Copied file from S3 '{}' to local '{}'".format(fname, local_path))
    #     model = joblib.load(local_path)
    #     return model

    # --- listdir set of functions ----
    def listdir(self, path):
        return self.listdir_cluster(path) if self.is_s3_path(path) else self.listdir_local(path)

    @staticmethod
    def listdir_local(path):
        return os.listdir(path)

    @staticmethod
    def listdir_cluster(path):  # TODO: rename to listdir_s3, same for similar functions from FS_Ops_Dispatcher
        # TODO: better handle invalid path. Crashes with "TypeError: 'NoneType' object is not iterable" at last line.
        if path.startswith('s3://'):
            s3_root = 's3://'
        elif path.startswith('s3a://'):
            s3_root = 's3a://'  # necessary when pulling S3 to local automatically from spark.
        else:
            raise ValueError('Problem with path. Pulling from s3, it should start with "s3://" or "s3a://". Path is: {}'.format(path))
        fname_parts = path.split(s3_root)[1].split('/')
        bucket_name = fname_parts[0]
        prefix = '/'.join(fname_parts[1:])
        client = boto3.Session(profile_name='default').client('s3')
        paginator = client.get_paginator('list_objects')
        objects = paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        paths = [item['Prefix'].split('/')[-2] for item in objects.search('CommonPrefixes')]
        return paths

    # --- dir_exist set of functions ----
    def dir_exist(self, path):
        return self.dir_exist_cluster(path) if self.is_s3_path(path) else self.dir_exist_local(path)

    @staticmethod
    def dir_exist_local(path):
        return os.path.isdir(path)

    @staticmethod
    def dir_exist_cluster(path):
        raise NotImplementedError

    # --- load_pandas set of functions ----

    def load_pandas(self, fname, file_type, read_func, read_kwargs):
        return self.load_pandas_cluster(fname, file_type, read_func, read_kwargs) if self.is_s3_path(fname) else self.load_pandas_local(fname, file_type, read_func, read_kwargs)

    @staticmethod
    def load_pandas_local(fname, file_type, read_func, read_kwargs):
        return load_df(fname, file_type, read_func, read_kwargs)

    def load_pandas_cluster(self, fname, file_type, read_func, read_kwargs):
        # import put here below to avoid loading it when working in local only.
        from cloudpathlib import CloudPath

        bucket_name, bucket_fname, fname_parts = self.split_s3_path(fname)
        local_path = 'tmp/s3_copy_' + fname_parts[-1]
        cp = CloudPath(fname)  # TODO: add way to load it with specific profile_name or client, as in "s3c = boto3.Session(profile_name='default').client('s3')"
        logger.info("Copying files from S3 '{}' to local '{}'. May take some time.".format(fname, local_path))
        local_pathlib = cp.download_to(local_path)
        local_path = local_path + '/' if local_pathlib.is_dir() else local_path
        logger.info("File copy finished")
        df = load_df(local_path, file_type, read_func, read_kwargs)
        return df

    # --- save_pandas set of functions ----

    def save_pandas(self, df, fname, save_method, save_kwargs):
        return self.save_pandas_cluster(df, fname, save_method, save_kwargs) if self.is_s3_path(fname) else self.save_pandas_local(df, fname, save_method, save_kwargs)

    @staticmethod
    def save_pandas_local(df, fname, save_method, save_kwargs):
        return save_pandas_local(df, fname, save_method, save_kwargs)

    def save_pandas_cluster(self, df, fname, save_method, save_kwargs):
        # code below can be simplified using "df.to_csv(fname, **save_kwargs)", relying on s3fs library, but implies lots of dependencies, that break in cloud run.
        bucket_name, bucket_fname, fname_parts = self.split_s3_path(fname)
        with StringIO() as file_buffer:
            save_pandas_local(df, file_buffer, save_method, save_kwargs)
            s3c = boto3.Session(profile_name='default').client('s3')
            response = s3c.put_object(Bucket=bucket_name, Key=bucket_fname, Body=file_buffer.getvalue())

        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response_code == 200:
            logger.info("Created file in S3: {}".format(fname))
        else:
            raise Exception("S3 couldn't be sent to S3")
        sleep(1)  # Prevent ThrottlingException
        return df


class Cred_Ops_Dispatcher():
    def retrieve_secrets(self, storage, aws_creds='/yaetos/connections', local_creds='conf/connections.cfg'):
        creds = self.retrieve_secrets_cluster(aws_creds) if storage == 's3' else self.retrieve_secrets_local(local_creds)
        return creds

    @staticmethod
    def retrieve_secrets_cluster(creds):
        client = boto3.Session(profile_name='default').client('secretsmanager')

        response = client.get_secret_value(SecretId=creds)
        logger.info('Read aws secret, secret_id:' + creds)
        logger.debug('get_secret_value response: ' + str(response))
        content = response['SecretString']

        fake_handle = StringIO(content)
        config = ConfigParser()
        config.readfp(fake_handle)
        return config

    @staticmethod
    def retrieve_secrets_local(creds):
        config = ConfigParser()
        assert os.path.isfile(creds)
        config.read(creds)
        return config
