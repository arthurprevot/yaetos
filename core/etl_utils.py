"""
Helper functions. Setup to run locally and on cluster.
"""

import sys
import inspect
import yaml
from datetime import datetime
import os
import boto3
import argparse
from time import time
import StringIO


JOBS_METADATA_FILE = 'conf/jobs_metadata.yml'
JOBS_METADATA_LOCAL_FILE = 'conf/jobs_metadata_local.yml'
CLUSTER_APP_FOLDER = '/home/hadoop/app/'


class etl(object):
    def transform(self, **app_args):
        raise NotImplementedError

    def etl(self, sc, sc_sql, app_name, args):
        start_time = time()
        self.sc = sc
        self.sc_sql = sc_sql
        self.app_name = app_name
        self.args = args
        self.set_path()

        loaded_datasets = self.load_inputs()
        output = self.transform(**loaded_datasets)
        self.save(output)

        end_time = time()
        elapsed = end_time - start_time
        self.save_metadata(elapsed)
        return output

    def commandline_launch(self, **args):
        """
        This function is used to deploy the script to aws and run it there or to run it locally.
        When deployed on cluster, this function is called again to run the script from the cluster.
        The inputs should not be dependent on whether the job is run locally or deployed to cluster as it is used for both.
        """
        parser = self.define_commandline_args()
        cmd_args = parser.parse_args()
        args.update(cmd_args.__dict__)  # commandline arguments take precedence over function ones.
        if args['execution'] == 'run':
            self.launch_run_mode(**args)
        elif args['execution'] == 'deploy':
            self.launch_deploy_mode(**args)

    @staticmethod
    def define_commandline_args():
        # Defined here separatly for overridability.
        parser = argparse.ArgumentParser()
        parser.add_argument("-e", "--execution", default='run', help="Choose 'run' (default) or 'deploy'.", choices=set(['deploy', 'run'])) # comes from cmd line since value is set when running on cluster
        parser.add_argument("-l", "--storage", default='local', help="Choose 'local' (default) or 's3'.", choices=set(['local', 's3'])) # comes from cmd line since value is set when running on cluster
        parser.add_argument("-a", "--aws_setup", default='dev', help="Choose aws setup from conf/config.cfg, typically 'prod' or 'dev'. Only relevant if choosing to deploy to a cluster.")
        # For later : --job_metadata_file, --machines, to be integrated only as a way to overide values from file.
        return parser

    def launch_run_mode(self, **args):
        # Load spark here instead of module to remove dependency on spark when only deploying code to aws.
        from pyspark import SparkContext
        from pyspark.sql import SQLContext
        app_name = self.get_app_name()
        sc = SparkContext(appName=app_name)
        sc_sql = SQLContext(sc)
        self.etl(sc, sc_sql, app_name, args)

    def launch_deploy_mode(self, aws_setup, **app_args):
        # Load deploy lib here instead of module to remove dependency on it when running code locally
        from core.deploy import DeployPySparkScriptOnAws
        app_file = inspect.getfile(self.__class__)
        DeployPySparkScriptOnAws(app_file=app_file, aws_setup=aws_setup, **app_args).run()

    def get_app_name(self):
        # Isolated in function for overridability
        return self.__class__.__name__

    def set_path(self):
        meta_file = CLUSTER_APP_FOLDER+JOBS_METADATA_FILE if self.args['storage']=='s3' else JOBS_METADATA_LOCAL_FILE
        yml = self.load_meta(meta_file)
        self.INPUTS = yml[self.app_name]['inputs']  # TODO: add error handling to deal with KeyError when name not found in jobs_metadata.
        self.OUTPUT = yml[self.app_name]['output']

    def load_inputs(self):
        app_args = {}
        for item in self.INPUTS.keys():
            path = self.INPUTS[item]['path']
            if '{latest}' in path:
                upstream_path = path.split('{latest}')[0]
                paths = self.listdir(upstream_path)
                latest_date = max(paths)
                path = path.format(latest=latest_date)

            if self.INPUTS[item]['type'] == 'txt':
                app_args[item] = self.sc.textFile(path)
            elif self.INPUTS[item]['type'] == 'csv':
                app_args[item] = self.sc_sql.read.csv(path, header=True)
                app_args[item].createOrReplaceTempView(item)
            elif self.INPUTS[item]['type'] == 'parquet':
                app_args[item] = self.sc_sql.read.parquet(path)
                app_args[item].createOrReplaceTempView(item)
        return app_args

    def save(self, output):
        path = self.OUTPUT['path']
        if '{now}' in path:
            current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S_utc')
            path = path.format(now=current_time)

        # TODO: deal with cases where "output" is df when expecting rdd and vice versa, or at least raise issue in a cleaner way.
        if self.OUTPUT['type'] == 'txt':
            output.saveAsTextFile(path)
        elif self.OUTPUT['type'] == 'parquet':
            output.write.parquet(path)
        elif self.OUTPUT['type'] == 'csv':
            output.write.csv(path)

        print 'Wrote output to ',path
        self.path = path

    def save_metadata(self, elapsed):
        fname = self.path + 'metadata.txt'
        content = """
            -- name: %s
            -- time (s): %s
            -- cluster_setup : TBD
            -- input folders : TBD
            -- output folder : TBD
            -- github hash: TBD
            -- code: TBD
            """%(self.app_name, elapsed)
        self.save_metadata_cluster(fname, content) if self.args['storage']=='s3' else self.save_metadata_local(fname, content)

    @staticmethod
    def save_metadata_local(fname, content):
        fh = open(fname, 'w')
        fh.write(content)
        fh.close()

    @staticmethod
    def save_metadata_cluster(fname, content):
        bucket_name = fname.split('s3://')[1].split('/')[0]  # TODO: remove redundancy
        bucket_fname = '/'.join(fname.split('s3://')[1].split('/')[1:])  # TODO: remove redundancy
        fake_handle = StringIO.StringIO(content)
        s3c = boto3.client('s3')
        s3c.put_object(Bucket=bucket_name, Key=bucket_fname, Body=fake_handle.read())

    def listdir(self, path):
        return self.listdir_cluster(path) if self.args['storage']=='s3' else self.listdir_local(path)

    @staticmethod
    def listdir_local(path):
        return os.listdir(path)

    @staticmethod
    def listdir_cluster(path):
        bucket_name = path.split('s3://')[1].split('/')[0]
        prefix = '/'.join(path.split('s3://')[1].split('/')[1:])
        client = boto3.client('s3')
        objects = client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        paths = [item['Prefix'].split('/')[-2] for item in objects.get('CommonPrefixes')]
        return paths

    def query(self, query_str):
        print 'Query string:', query_str
        return self.sc_sql.sql(query_str)

    @staticmethod
    def load_meta(fname):
        with open(fname, 'r') as stream:
            yml = yaml.load(stream)
        return yml
