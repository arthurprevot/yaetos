"""
Helper functions. Setup to run locally and on cluster.
"""
# TODO:
# - add logger
# - add linter
# - extract yml ops to separate class for reuse in Flow()
# - setup command line args defaults to None so they can be overriden only if set in commandline and default would be in config file or jobs_metadata.yml
# - make yml look more like command line info, with path of python script.
# - finish _metadata.txt file content.
# - make raw functions available to raw spark jobs.
# - refactor to have schedule input managed first and passed as args that can be overiden by commandline, so easier to change input output from commandline
# - allow running job with passed inputs from memory easily, without relying on job_param_file and without having to specify input path at all, for test purposes (and better separation of concern.)


import sys
import inspect
import yaml
from datetime import datetime
import os
import boto3
import argparse
from time import time
import StringIO
import networkx as nx
import random
import pandas as pd
import os


JOBS_METADATA_FILE = 'conf/jobs_metadata.yml'
JOBS_METADATA_LOCAL_FILE = 'conf/jobs_metadata_local.yml'
CLUSTER_APP_FOLDER = '/home/hadoop/app/'
LOCAL_APP_FOLDER = os.environ.get('PYSPARK_AWS_ETL_HOME', '') #+ '/'


class ETL_Base(object):
    tabular_types = ('csv', 'parquet', 'df')

    def __init__(self, args={}):
        self.args = args

    def set_job_params(self, loaded_inputs={}):
        """ Setting the params from yml or from commandline args if available."""
        self.set_job_file()
        self.set_job_name(self.job_file)  # differs from app_name when one spark app runs several jobs.


        job_params_file = self.args.get('job_params_file')
        # print '#### self.args.get(job_params_file)', self.args.get('job_params_file')

        if job_params_file:
            self.set_job_yml()

        # Inputs
        inputs_in_args = len([item for item in self.args.keys() if item.startswith('input_')]) >= 1
        if inputs_in_args:
            self.INPUTS = {key.replace('input_', ''): {'path': val, 'type': 'df'} for key, val in self.args.iteritems() if key.startswith('input_')}
        elif job_params_file:
            self.INPUTS = self.job_yml['inputs']
        elif loaded_inputs:
            self.INPUTS = {key: {'path': val, 'type': 'df'} for key, val in loaded_inputs.iteritems()}
        else:
            raise Error("No input given")

        # Outputs
        output_in_args = len([item for item in self.args.keys() if item == 'output']) >= 1
        if output_in_args:
            self.OUTPUT = self.args['output']
        elif job_params_file:
            self.OUTPUT = self.job_yml['output']

        # Frequency
        if self.args.get('frequency'):
            self.frequency = self.args.get('frequency')
        elif job_params_file:
            self.frequency = self.job_yml.get('frequency', None)
        else:
            self.frequency = None

        self.set_is_incremental()

    def etl(self, sc, sc_sql, loaded_inputs={}):
        """ Main function that loads inputs, run transform, save output."""
        start_time = time()

        output = self.etl_no_io(sc, sc_sql, loaded_inputs)
        self.save(output)

        end_time = time()
        elapsed = end_time - start_time
        self.save_metadata(elapsed)
        return output

    def etl_no_io(self, sc, sc_sql, loaded_inputs={}):
        """ Funcion to load inputs (including from live vars) and run transform. No output to disk.
        Having this code isolated is useful for cases with no I/O possible, like testing."""
        self.set_job_params(loaded_inputs)
        self.sc = sc
        self.sc_sql = sc_sql
        self.app_name = sc.appName
        print "-------\nStarting running job '{}' in spark app '{}'.".format(self.job_name, self.app_name)

        loaded_datasets = self.load_inputs(loaded_inputs)
        output = self.transform(**loaded_datasets)
        return output

    def transform(self, **app_args):
        raise NotImplementedError

    def set_job_file(self):
        self.job_file = self.args.get('job_file', inspect.getsourcefile(self.__class__))  # getsourcefile works when run from final job file.

    def set_job_name(self, job_file):
        # when run from Flow(), job_file is full path. When run from ETL directly, job_file is "jobs/..." .
        # TODO change this hacky way to deal with it.
        self.job_name = job_file \
            .replace(CLUSTER_APP_FOLDER+'jobs/','') \
            .replace(CLUSTER_APP_FOLDER+'scripts.zip/jobs/','') \
            .replace(LOCAL_APP_FOLDER+'jobs/','') \
            .replace('jobs/','')  # has to be last

    @staticmethod
    def get_job_class(job_name):
        name_import = job_name.replace('/','.').replace('.py','')
        import_cmd = "from jobs.{} import Job".format(name_import)
        exec(import_cmd)
        return Job

    def set_job_yml(self):
        meta_file = CLUSTER_APP_FOLDER+JOBS_METADATA_FILE if self.args['storage']=='s3' else JOBS_METADATA_LOCAL_FILE
        yml = self.load_meta(meta_file)
        print 'Loaded job param file: ', meta_file
        try:
            self.job_yml = yml[self.job_name]
        except KeyError:
            raise KeyError("Your job '{}' can't be found in jobs_metadata file '{}'. Add it there or make sure the name matches".format(self.job_name, meta_file))

    def set_is_incremental(self):
        self.is_incremental = any([self.INPUTS[item].get('inc_field', None) is not None for item in self.INPUTS.keys()])

    def load_inputs(self, loaded_inputs):
        app_args = {}
        # print '###, self.INPUTS', self.INPUTS
        for item in self.INPUTS.keys():

            # Load from memory if available
            if item in loaded_inputs.keys():
                app_args[item] = loaded_inputs[item]
                print "Input '{}' passed in memory from a previous job.".format(item)
                continue

            # Load from disk
            path = self.INPUTS[item]['path']
            path = Path_Handler(path).expand_later(self.args['storage'])
            app_args[item] = self.load_data(path, self.INPUTS[item]['type'])
            print "Input '{}' loaded from files '{}'.".format(item, path)

        if self.is_incremental:
            app_args = self.filter_incremental_inputs(app_args)

        self.sql_register(app_args)
        return app_args

    def filter_incremental_inputs(self, app_args):
        min_dt = self.get_output_max_timestamp()

        # Get latest timestamp in common across incremental inputs
        maxes = []
        for item in app_args.keys():
            input_is_tabular = self.INPUTS[item]['type'] in self.tabular_types
            inc = self.INPUTS[item].get('inc_field', None)
            if input_is_tabular and inc:
                max_dt = app_args[item].agg({inc: "max"}).collect()[0][0]
                maxes.append(max_dt)
        max_dt = min(maxes) if len(maxes)>0 else None

        # Filter
        for item in app_args.keys():
            input_is_tabular = self.INPUTS[item]['type'] in self.tabular_types
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
            input_is_tabular = hasattr(app_args[item], "rdd")  # assuming DataFrame will keep 'rdd' attribute
            # ^ better than using self.INPUTS[item]['type'] in self.tabular_types since doesn't require 'type' being defined.
            if input_is_tabular:
                app_args[item].createOrReplaceTempView(item)

    def load_data(self, path, path_type):
        if path_type == 'txt':
            return self.sc.textFile(path)
        elif path_type == 'csv':
            return self.sc_sql.read.csv(path, header=True)
        elif path_type == 'parquet':
            return self.sc_sql.read.parquet(path)
        else:
            supported = ['txt', 'csv', 'parquet']  # TODO: register types differently without duplicating
            raise Exception("Unsupported file type '{}' for path '{}'. Supported types are: {}. ".format(path_type, path, supported))

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
        path = Path_Handler(self.OUTPUT['path']).expand_now()

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
        fname = self.path + '_metadata.txt'
        content = """
            -- app_name: %s
            -- job_name: %s
            -- time (s): %s
            -- cluster_setup : TBD
            -- input folders : TBD
            -- output folder : TBD
            -- github hash: TBD
            -- code: TBD
            """%(self.app_name, self.job_name, elapsed)
        FS_Ops_Dispatcher().save_metadata(fname, content, self.args['storage'])

    def query(self, query_str):
        print 'Query string:\n', query_str
        return self.sc_sql.sql(query_str)

    @staticmethod
    def load_meta(fname):
        with open(fname, 'r') as stream:
            yml = yaml.load(stream)
        return yml


class FS_Ops_Dispatcher():
    def save_metadata(self, fname, content, storage):
        self.save_metadata_cluster(fname, content) if storage=='s3' else self.save_metadata_local(fname, content)

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

    def listdir(self, path, storage):
        return self.listdir_cluster(path) if storage=='s3' else self.listdir_local(path)

    @staticmethod
    def listdir_local(path):
        return os.listdir(path)

    @staticmethod
    def listdir_cluster(path):
        # TODO: better handle invalid path. Crashes with "TypeError: 'NoneType' object is not iterable" at last line.
        bucket_name = path.split('s3://')[1].split('/')[0]
        prefix = '/'.join(path.split('s3://')[1].split('/')[1:])
        client = boto3.client('s3')
        objects = client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        paths = [item['Prefix'].split('/')[-2] for item in objects.get('CommonPrefixes')]
        return paths

    def dir_exist(self, path, storage):
        return self.dir_exist_cluster(path) if storage=='s3' else self.dir_exist_local(path)

    @staticmethod
    def dir_exist_local(path):
        return os.path.isdir(path)

    @staticmethod
    def dir_exist_cluster(path):
        raise "Not implemented"


class Path_Handler():
    def __init__(self, path):
        self.path = path

    def expand_later(self, storage):
        path = self.path
        if '{latest}' in path:
            upstream_path = path.split('{latest}')[0]
            paths = FS_Ops_Dispatcher().listdir(upstream_path, storage)
            latest_date = max(paths)
            path = path.format(latest=latest_date)
        return path

    def expand_now(self):
        path = self.path
        if '{now}' in path:
            current_time = datetime.utcnow().strftime('date%Y%m%d_time%H%M%S_utc')
            path = path.format(now=current_time)
        return path

    def get_base(self):
        if '{latest}' in self.path:
            return self.path.split('{latest}')[0]
        elif '{now}' in self.path:
            return self.path.split('{now}')[0]
        else:
            return self.path


class Commandliner():
    def __init__(self, Job, **args):
        self.set_commandline_args(args)
        if not args['deploy']:
            self.launch_run_mode(Job, self.args)
        else:
            job_file = Job(self.args).job_file
            self.launch_deploy_mode(job_file, **self.args)

    def set_commandline_args(self, args):
        """Command line arguments take precedence over function ones."""
        self.args = args
        parser = self.define_commandline_args()
        cmd_args = parser.parse_args()
        self.args.update(cmd_args.__dict__)

    @staticmethod
    def define_commandline_args():
        # Defined here separatly for overridability.
        parser = argparse.ArgumentParser()
        parser.add_argument("-d", "--deploy", action='store_true', help="Deploy the job to a cluster and run it there instead of running it now locally.") # comes from cmd line since value is set when running on cluster
        parser.add_argument("-j", "--job_params_file", action='store_false', help="Grab job params from the schedule file or not. If not, need to specify every param. Default TBD")  # for later. TODO: set TBD
        parser.add_argument("-l", "--storage", default='local', help="Choose 'local' (default) or 's3'.", choices=set(['local', 's3'])) # comes from cmd line since value is set when running on cluster
        parser.add_argument("-a", "--aws_setup", default='perso', help="Choose aws setup from conf/config.cfg, typically 'prod' or 'dev'. Only relevant if choosing to deploy to a cluster.")
        parser.add_argument("-x", "--dependencies", action='store_true', help="Run the job dependencies and then the job itself")
        # For later : --job_metadata_file, --machines, --inputs, --output, to be integrated only as a way to overide values from file.
        return parser

    def launch_run_mode(self, Job, args):
        # Load spark here instead of at module level to remove dependency on spark when only deploying code to aws.
        from pyspark import SparkContext
        from pyspark.sql import SQLContext
        job = Job(args)
        job.set_job_file()
        job.set_job_name(job.job_file)
        app_name = job.job_name
        sc = SparkContext(appName=app_name)
        sc_sql = SQLContext(sc)
        if not self.args['dependencies']:
            job.etl(sc, sc_sql)
        else:
            Flow(sc, sc_sql, args, app_name)

    def launch_deploy_mode(self, job_file, aws_setup, **app_args):
        # Load deploy lib here instead of at module level to remove dependency on it when running code locally
        from core.deploy import DeployPySparkScriptOnAws
        DeployPySparkScriptOnAws(app_file=job_file, aws_setup=aws_setup, **app_args).run()


class Flow():
    def __init__(self, sc, sc_sql, args, app_name):
        self.app_name = app_name
        storage = args['storage']
        df = self.create_connections_jobs(storage)
        graph = self.create_global_graph(df)  # top to bottom
        tree = self.create_local_tree(graph, nx.DiGraph(), app_name) # bottom to top
        leafs = self.get_leafs(tree, leafs=[]) # bottom to top
        print 'Sequence of jobs to be run', leafs

        # load all job classes and run them
        df = {}
        for job_name in leafs:
            if job_name.endswith('.sql'):
                args['sql_file'] = 'jobs/' + job_name

            Job = self.get_job_class(job_name)
            job = Job(args)

            loaded_inputs = {}
            for in_name, in_properties in job.job_yml['inputs'].iteritems():
                if in_properties.get('from'):
                    loaded_inputs[in_name] = df[in_properties['from']]
            df[job_name] = job.etl(sc, sc_sql, loaded_inputs)

    @staticmethod
    def get_job_class(name):
        if name.endswith('.py'):
            return ETL_Base.get_job_class(name)
        elif name.endswith('.sql'):
            from core.sql_job import SQL_Job  # can't be on top because of circular dependencies
            return SQL_Job.get_job_class(name)
        else:
            raise Exception("Extension not recognized")

    def create_connections_jobs(self, storage):
        meta_file = CLUSTER_APP_FOLDER+JOBS_METADATA_FILE if storage=='s3' else JOBS_METADATA_LOCAL_FILE # TODO: don't repeat from etl_base
        yml = ETL_Base.load_meta(meta_file)

        connections = []
        for job_name, job_meta in yml.iteritems():
            dependencies = job_meta.get('dependencies') or []
            for dependency in dependencies:
                row = {'source_job': dependency, 'destination_job': job_name}
                connections.append(row)

        return pd.DataFrame(connections)

    def create_global_graph(self, df):
        """ Directed Graph from source to target. df must contain 'source_dataset' and 'target_dataset'.
        All other fields are attributed to target."""
        DG = nx.DiGraph()
        for ii, item in df.iterrows():
            item = item.to_dict()
            source_dataset = item.pop('source_job')
            target_dataset = item.pop('destination_job')
            item.update({'name':target_dataset})

            DG.add_edge(source_dataset, target_dataset)
            DG.add_node(source_dataset, {'name':source_dataset})
            DG.add_node(target_dataset, item)
        return DG

    def create_local_tree(self, DG, tree, ref_node):
        """ Builds tree recursively. Uses graph data structure but enforces tree to simplify downstream."""
        nodes = DG.predecessors(ref_node)
        tree.add_node(ref_node, DG.node[ref_node])
        for item in nodes:
            if not tree.has_node(item):
                tree.add_edge(ref_node, item)
                tree.add_node(item, DG.node[item])
                self.create_local_tree(DG, tree, item)
        return tree

    def get_leafs(self, tree, leafs):
        """Recursive function to extract all leafs in order out of tree.
        Each pass, jobs are moved from "tree" to "leafs" variables until done.
        """
        cur_leafs = [node for node in tree.nodes() if tree.in_degree(node)!=0 and tree.out_degree(node)==0]
        leafs += cur_leafs

        for leaf in cur_leafs:
            tree.remove_node(leaf)

        if len(tree.nodes()) >= 2:
            self.get_leafs(tree, leafs)

        return leafs + tree.nodes()
