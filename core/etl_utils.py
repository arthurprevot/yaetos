"""
Helper functions. Setup to run locally and on cluster.
"""

import sys
import inspect
import yaml
from datetime import datetime
import os
import boto3


JOBS_METADATA_FILE = 'conf/jobs_metadata.yml'
CLUSTER_APP_FOLDER = '/home/hadoop/app/'
# TODO: may be get inputs and output as param of python file launch to be more explicit and flexible

class etl(object):
    def run(self, sc, **kwargs):
        raise NotImplementedError

    def runner(self, sc, sc_sql):
        self.sc = sc
        self.sc_sql = sc_sql

        run_args = self.load_inputs()
        output = self.run(**run_args)
        self.save(output)
        return output

    def set_path(self, fname_schedule, app_name):
        """Can override this method to force paths regardless of job_meta file."""
        yml = self.load_schedule(fname_schedule)
        self.INPUTS = yml[app_name]['inputs']
        self.OUTPUT = yml[app_name]['output']

    def load_inputs(self):
        run_args = {}
        for item in self.INPUTS.keys():
            path = self.INPUTS[item]['path']
            if '{latest}' in path:
                upstream_path = path.split('{latest}')[0]
                paths = self.listdir(upstream_path)
                latest_date = max(paths)
                path = path.format(latest=latest_date)

            if self.INPUTS[item]['type'] == 'txt':
                run_args[item] = self.sc.textFile(path)
            elif self.INPUTS[item]['type'] == 'csv':
                run_args[item] = self.sc_sql.read.csv(path, header=True)
                run_args[item].createOrReplaceTempView(item)
            elif self.INPUTS[item]['type'] == 'parquet':
                run_args[item] = self.sc_sql.read.parquet(path)
                run_args[item].createOrReplaceTempView(item)
        return run_args

    def save(self, output):
        path = self.OUTPUT['path']
        if '{now}' in path:
            current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S_utc')
            path = path.format(now=current_time)

        # TODO deal with cases where "output" is df when expecting rdd and vice versa, or at least raise issue in a cleaner way.
        if self.OUTPUT['type'] == 'txt':
            output.saveAsTextFile(path)
        elif self.OUTPUT['type'] == 'parquet':
            output.write.parquet(path)
        elif self.OUTPUT['type'] == 'csv':
            output.write.csv(path)

        print 'Wrote output to ',path

    def listdir(self, path):
        # TODO: make function cleaner
        # For local path
        if not path.lower().startswith('s3://'):
            return os.listdir(path)

        # For s3 path
        bucket_name = path.split('s3://')[1].split('/')[0]
        prefix = '/'.join(path.split('s3://')[1].split('/')[1:])
        client = boto3.client('s3')
        objects = client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        paths = [item['Prefix'].split('/')[-2] for item in objects.get('CommonPrefixes')]
        return paths

    def query(self, query_str):
        print 'Query string:', query_str
        return self.sc_sql.sql(query_str)

    def load_schedule(self, fname):
        with open(fname, 'r') as stream:
            yml = yaml.load(stream)
        return yml


def launch(job_class, aws):
    """This function input should not be dependent on whether the job is run locally or deployed to cluster."""
    process = sys.argv[1] if len(sys.argv) > 1 else 'run'
    location = sys.argv[2] if len(sys.argv) > 2 else 'local'
    app_file = inspect.getfile(job_class)
    app_name = job_class.__name__
    if process == 'run':
        from pyspark import SparkContext
        from pyspark.sql import SQLContext
        sc = SparkContext(appName=app_name)
        sc_sql = SQLContext(sc)
        meta_file = CLUSTER_APP_FOLDER+JOBS_METADATA_FILE if location=='cluster' else 'conf/jobs_metadata_local.yml'
        myjob = job_class()
        myjob.set_path(meta_file, app_name)
        myjob.runner(sc, sc_sql)
    elif process == 'deploy':
        from core.deploy import DeployPySparkScriptOnAws
        DeployPySparkScriptOnAws(app_file=app_file, setup=aws).run()
