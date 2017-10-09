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


class etl_base(object):
    def transform(self, **app_args):
        raise NotImplementedError

    def etl(self, sc, sc_sql, app_name, args):
        start_time = time()
        self.sc = sc
        self.sc_sql = sc_sql
        self.app_name = app_name
        self.args = args
        self.set_app_yml()
        self.set_paths()
        self.set_is_incremental()
        self.set_frequency()

        loaded_datasets = self.load_inputs()
        output = self.transform(**loaded_datasets)
        self.save(output)

        end_time = time()
        elapsed = end_time - start_time
        self.save_metadata(elapsed)
        return output

    def commandline_launch(self, **args):
        """
        This function is used to run the job locally or deploy it to aws and run it there.
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
        app_name = self.get_app_name(args)
        sc = SparkContext(appName=app_name)
        sc_sql = SQLContext(sc)
        self.etl(sc, sc_sql, app_name, args)

    def launch_deploy_mode(self, aws_setup, **app_args):
        # Load deploy lib here instead of module to remove dependency on it when running code locally
        from core.deploy import DeployPySparkScriptOnAws
        DeployPySparkScriptOnAws(app_file=self.get_app_file(), aws_setup=aws_setup, **app_args).run()

    def get_app_name(self, args):
        # Isolated in function for overridability
        app_file = self.get_app_file()
        return app_file.split('/')[-1].replace('.py','')  # TODO make better with os.path functions.

    def get_app_file(self):
        return inspect.getfile(self.__class__)

    def set_app_yml(self):
        meta_file = CLUSTER_APP_FOLDER+JOBS_METADATA_FILE if self.args['storage']=='s3' else JOBS_METADATA_LOCAL_FILE
        yml = self.load_meta(meta_file)
        try:
            self.app_yml = yml[self.app_name]
        except KeyError:
            raise KeyError("Your app ({}) can't be found in jobs_metadata file ({}). Add it there or make sure the name matches".format(self.app_name, meta_file))

    def set_paths(self):
        self.INPUTS = self.app_yml['inputs']
        self.OUTPUT = self.app_yml['output']

    def set_is_incremental(self):
        self.is_incremental = any([self.INPUTS[item].get('inc_field', None) is not None for item in self.INPUTS.keys()])

    def set_frequency(self):
        self.frequency = self.app_yml.get('frequency', None)

    def load_inputs(self):
        app_args = {}
        for item in self.INPUTS.keys():
            path = self.INPUTS[item]['path']
            if '{latest}' in path:
                upstream_path = path.split('{latest}')[0]
                paths = self.listdir(upstream_path)
                latest_date = max(paths)
                path = path.format(latest=latest_date)
            app_args[item] = self.load_data(path, self.INPUTS[item]['type'])

        if self.is_incremental:
            app_args = self.filter_incremental_inputs(app_args)

        self.sql_register(app_args)
        return app_args

    def filter_incremental_inputs(self, app_args):
        min_dt = self.get_output_max_timestamp()

        # Get latest timestamp in common across incremental inputs
        maxes = []
        for item in app_args.keys():
            input_is_tabular = self.INPUTS[item]['type'] in ('csv', 'parquet')  # TODO: register as part of function
            inc = self.INPUTS[item].get('inc_field', None)
            if input_is_tabular and inc:
                max_dt = app_args[item].agg({inc: "max"}).collect()[0][0]
                maxes.append(max_dt)
        max_dt = min(maxes) if len(maxes)>0 else None

        # Filter
        for item in app_args.keys():
            input_is_tabular = self.INPUTS[item]['type'] in ('csv', 'parquet')  # TODO: register as part of function
            inc = self.INPUTS[item].get('inc_field', None)
            if inc:
                if input_is_tabular:
                    inc_type = {k:v for k, v in app_args[item].dtypes}[inc]
                    print "Input dataset '{}' will be filtered for min_dt={} max_dt={}".format(item, min_dt, max_dt)
                    if min_dt:
                        # min_dt = to_date(lit(s)).cast(TimestampType()  # TODO: deal with dt type, as coming from parquet
                        app_args[item] = app_args[item].filter(app_args[item][inc] > min_dt)
                    if max_dt:
                        app_args[item] = app_args[item].filter(app_args[item][inc] <= max_dt)
                else:
                    raise "Incremental loading is not supported for unstructured input. You need to handle the incremental logic in the job code."
        return app_args

    def sql_register(self, app_args):
        for item in app_args.keys():
            input_is_tabular = self.INPUTS[item]['type'] in ('csv', 'parquet')  # TODO: register as part of function
            if input_is_tabular:
                app_args[item].createOrReplaceTempView(item)

    def load_data(self, path, path_type):
        if path_type == 'txt':
            return self.sc.textFile(path)
        elif path_type == 'csv':
            return self.sc_sql.read.csv(path, header=True)
        elif path_type == 'parquet':
            return self.sc_sql.read.parquet(path)

    def get_output_max_timestamp(self):
        path = self.OUTPUT['path']
        path += '*' # to go into subfolders
        try:
            df = self.load_data(path, self.OUTPUT['type'])
        except Exception as e:  # TODO: don't catch all
            print "Previous increment could not be loaded or doesn't exist. It will be ignored. Folder '{}' failed loading with error '{}'.".format(path, e)
            return None

        dt = df.agg({self.OUTPUT['inc_field']: "max"}).collect()[0][0]
        print "Max timestamp of previous increment: '{}'".format(dt)
        return dt

    def save(self, output):
        path = self.OUTPUT['path']
        if '{now}' in path:
            current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S_utc')
            path = path.format(now=current_time)

        if self.is_incremental:
            current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S_utc')
            path += 'inc_%s/'%current_time

        # TODO: deal with cases where "output" is df when expecting rdd, or at least raise issue in a cleaner way.
        if self.OUTPUT['type'] == 'txt':
            output.saveAsTextFile(path)
        elif self.OUTPUT['type'] == 'parquet':
            output.write.parquet(path)
        elif self.OUTPUT['type'] == 'csv':
            output.write.option("header", "true").csv(path)

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

    def dir_exist(self, path):
        return self.dir_exist_cluster(path) if self.args['storage']=='s3' else self.dir_exist_local(path)

    @staticmethod
    def dir_exist_local(path):
        return os.path.isdir(path)

    @staticmethod
    def dir_exist_cluster(path):
        raise "Not implemented"

    def query(self, query_str):
        print 'Query string:', query_str
        return self.sc_sql.sql(query_str)

    @staticmethod
    def load_meta(fname):
        with open(fname, 'r') as stream:
            yml = yaml.load(stream)
        return yml