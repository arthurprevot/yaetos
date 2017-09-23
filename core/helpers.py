"""
Helper functions. Setup to run locally and on cluster.
"""

import sys
import inspect
import yaml
from datetime import datetime
import os
import boto3


JOBS_METADATA_FILE = 'conf/scheduling.yml'
CLUSTER_APP_FOLDER = '/home/hadoop/app/'


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
            # print '####### path',path
            if '{latest}' in path:
                upstream_path = path.split('{latest}')[0]
                # paths = os.listdir(upstream_path)  # TODO make work on s3 path too.
                paths = self.listdir(upstream_path)  # TODO make work on s3 path too.
                # print '#### all',paths
                latest_date = max(paths)
                path = path.format(latest=latest_date)
                # print '#### latest',path

            # import ipdb; ipdb.set_trace()

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
            # import ipdb; ipdb.set_trace()
            path = path.format(now=current_time)

        if self.OUTPUT['type'] == 'txt':
            # TODO deal with case where it is df, or just raise issue if df.
            output.saveAsTextFile(path)
        elif self.OUTPUT['type'] == 'parquet':
            # TODO deal with case where it is rdd.
            output.write.parquet(path)
        elif self.OUTPUT['type'] == 'csv':
            # TODO deal with case where it is rdd.
            output.write.csv(path)

        print 'Wrote output to ',path

    def listdir(self, path):
        if not path.lower().startswith('s3://'):
            return os.listdir(path)

        # For s3 path
        # TODO: not scalable. May have issues if returns more than 1000 elements.
        # s3 = boto3.resource('s3')
        print '### path', path
        bucket_name = path.split('s3://')[1].split('/')[0]
        print '### bucket_name', bucket_name
        # bucket = s3.Bucket(name=bucket_name)
        prefix = '/'.join(path.split('s3://')[1].split('/')[1:])
        print '### prefix', prefix
        # objects = bucket.objects.filter(Prefix=prefix)
        client = boto3.client('s3')
        print '### client', client
        objects = client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        print '### objects', objects
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
        meta_file = CLUSTER_APP_FOLDER+JOBS_METADATA_FILE if location=='cluster' else 'conf/scheduling_local.yml'
        myjob = job_class()
        myjob.set_path(meta_file, app_name)
        myjob.runner(sc, sc_sql)
    elif process == 'deploy':
        from core.run import DeployPySparkScriptOnAws
        DeployPySparkScriptOnAws(app_file=app_file, setup=aws).run()
