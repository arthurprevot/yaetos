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


JOBS_METADATA_FILE = 'conf/jobs_metadata.yml'
JOBS_METADATA_LOCAL_FILE = 'conf/jobs_metadata_local.yml'
CLUSTER_APP_FOLDER = '/home/hadoop/app/'

class etl(object):
    def run(self, **app_args):
        raise NotImplementedError

    def run_handler(self, sc, sc_sql, location, **app_args):
        # start_time = time()
        self.sc = sc
        self.sc_sql = sc_sql
        self.location = location
        self.app_name = sc.appName
        self.set_path()

        loaded_datasets = self.load_inputs()
        app_args.update(loaded_datasets)
        output = self.run(**app_args)
        self.save(output)

        # end_time = time()
        # elapsed = end_time - start_time
        # self.save_metadata()
        return output

    def set_path(self):
        meta_file = CLUSTER_APP_FOLDER+JOBS_METADATA_FILE if self.location=='cluster' else JOBS_METADATA_LOCAL_FILE
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

    # def save_metadata(self):
    #     return self.save_metadata_cluster() if self.location=='cluster' else self.save_metadata_local()
    #
    # def save_metadata_local(self):
    #     content = "-- name: %s\n-- db_creds (#db_type#): %s\n-- time (s): %s\n-- query: \n%s\n-- end"%(name, db_type, elapsed, query_str)
    #     write_file(fname_sql, content)
    #
    # def save_metadata_cluster(self):
    #     content = "-- name: %s\n-- db_creds (#db_type#): %s\n-- time (s): %s\n-- query: \n%s\n-- end"%(name, db_type, elapsed, query_str)
    #     write_file(fname_sql, content)

    def listdir(self, path):
        return self.listdir_cluster(path) if self.location=='cluster' else self.listdir_local(path)

    @staticmethod
    def listdir_local(path):
        # TODO: make function clearer
        # For local path
        # if not path.lower().startswith('s3://'):  # TODO: use boolean set upstream to decide on local vs cluster.
            # return os.listdir(path)
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


# def write_file(fname, content):
#     fh = open(fname, 'w')
#     fh.write(content)
#     fh.close()


def launch(job_class, sql_job=False, **kwargs):
    """
    This function is used to deploy the script to aws and run it there or to run it locally.
    When deployed on cluster, this function is called again to run the script from the cluster.
    The inputs should not be dependent on whether the job is run locally or deployed to cluster as it is used for both.
    """
    # TODO: redo this function to clarify commandline args vs function args vs args to go into deploy or run mode.. could use kwargs to set params below if not overriden by commandline args.
    # TODO: look at adding input and output path as cmdline as a way to override schedule ones. or better differentiate cmdline args vs app_args

    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--execution", default='run', help="choose 'run' (default) or 'deploy_and_run'.", choices=set(['deploy_and_run', 'run'])) # comes from cmd line since value is set when running on cluster
    parser.add_argument("-l", "--location", default='local', help="choose 'local' (default) or 'cluster'.", choices=set(['local', 'cluster'])) # comes from cmd line since value is set when running on cluster
    # parser.add_argument("-m", "--job_metadata_file", default='conf/jobs_metadata.yml', help="To override repo job")  # TODO better integrate
    # parser.add_argument("-w", "--machines", default=2, help="To set number of instance . Only relevant if choosing to create a new cluster.")
    # parser.add_argument("-a", "--aws_setup", default='dev', help="asdf . Only relevant if choosing to deploy to a cluster.")
    if sql_job:
        parser.add_argument("-s", "--sql_file", help="path of sql file to run") # TODO: make mandatory
    args = parser.parse_args()

    app_args = {}
    if sql_job and args.sql_file is not None:
        app_args['sql_file']= args.sql_file  # TODO: add app_name and meta_file args there

    if args.execution == 'run':
        from pyspark import SparkContext
        from pyspark.sql import SQLContext
        app_name = job_class.__name__ if not sql_job else app_args['sql_file'].split('/')[-1].replace('.sql','')  # Quick and dirty, forces name of sql file to match schedule entry
        sc = SparkContext(appName=app_name)
        sc_sql = SQLContext(sc)
        job_class().run_handler(sc, sc_sql, args.location, **app_args)
    elif args.execution == 'deploy_and_run':
        from core.deploy import DeployPySparkScriptOnAws
        aws_setup = kwargs.get('aws_setup', 'dev')
        app_file = inspect.getfile(job_class)
        DeployPySparkScriptOnAws(app_file=app_file, aws_setup=aws_setup, **app_args).run()
