"""
Helper functions. Setup to run locally and on cluster.
"""
# TODO:
# - add linter
# - make yml look more like command line info, with path of python script.
# - finish _metadata.txt file content.
# - make raw functions available to raw spark jobs.
# - refactor to have schedule input managed first and passed as args that can be overiden by commandline, so easier to change input output from commandline
# - make use of "incremental" param in job_metadata files.
# - rewrite Job_Args_Parser(), see class, and add function to generate spark_submit args and cmdlines.
# - cleaner job args vs yml inputs. Functions shouldn't need both, and shouldn't rerun when already done. Could you inputs like "jobname_or_yml_obj"
# - add ability to change dev vs prod vs local base path in jobs_metadata
# - better check that db copy is in sync with S3.
# - way to run all jobs from 1 cmd line.
# - rename mode=EMR and EMR_Scheduled modes to deploy="EMR" and "EMR_Scheduled" and None, and use new "mode" arg so app knows in which mode it currently is.
# - make boxed_dependencies the default, as more conservative.
# - better integration of redshift based on spark instead of sqlalchemy.
# - add ability to sent a comment about the job through the commandline, like "debug run with condition x", "scheduled run"...


import sys
import inspect
import yaml
from datetime import datetime
import os
import boto3
import argparse
from time import time
from io import StringIO
import networkx as nx
import random
import pandas as pd
import os
import sys
from configparser import ConfigParser
import numpy as np
#from sklearn.externals import joblib  # TODO: re-enable later after fixing lib versions.
import gc
import core.logger as log
logger = log.setup_logging('Job')


JOBS_METADATA_FILE = 'conf/jobs_metadata.yml'
JOBS_METADATA_LOCAL_FILE = 'conf/jobs_metadata_local.yml'
AWS_CONFIG_FILE = 'conf/aws_config.cfg'
CONNECTION_FILE = 'conf/connections.cfg'
CLUSTER_APP_FOLDER = '/home/hadoop/app/'
LOCAL_APP_FOLDER = os.environ.get('PYSPARK_AWS_ETL_HOME', '') # PYSPARK_AWS_ETL_HOME set to end with '/'
LOCAL_JOB_REPO_FOLDER = os.environ.get('PYSPARK_AWS_ETL_JOBS_HOME', '')
AWS_SECRET_ID = '/yaetos/connections'
JOB_FOLDER = 'jobs/'
REDSHIFT_S3_TMP_DIR = "s3a://sandbox-arthur/yaetos/tmp_spark/"  # user setting. TODO: set from job_metadata.yml
PACKAGES_LOCAL = 'com.amazonaws:aws-java-sdk-pom:1.11.760,org.apache.hadoop:hadoop-aws:2.7.0,com.databricks:spark-redshift_2.11:2.0.1,org.apache.spark:spark-avro_2.11:2.4.0,mysql:mysql-connector-java:8.0.11'  # necessary for reading/writing to redshift and mysql using spark connector.
PACKAGES_EMR = 'com.databricks:spark-redshift_2.11:2.0.1,org.apache.spark:spark-avro_2.11:2.4.0,mysql:mysql-connector-java:8.0.11'  # necessary for reading/writing to redshift and mysql using spark connector.
JARS = 'https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.41.1065/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar'  # not available in public repo so cannot be put in "packages" var.


class ETL_Base(object):
    TABULAR_TYPES = ('csv', 'parquet', 'df', 'mysql')
    FILE_TYPES = ('csv', 'parquet', 'txt')
    SUPPORTED_TYPES = set(TABULAR_TYPES).union(set(FILE_TYPES)).union({'other', 'None'})

    def __init__(self, args={}):
        self.args = args

    def etl(self, sc, sc_sql, loaded_inputs={}):
        """ Main function. If incremental, reruns ETL process multiple time until
        fully loaded, otherwise, just run ETL once.
        It's a way to deal with case where full incremental rerun from scratch would
        require a larger cluster to build in 1 shot than the typical incremental.
        """
        self.set_job_params(loaded_inputs)  # TODO: check way to remove from here since already computed in Commandliner
        if not self.is_incremental:
            output = self.etl_one_pass(sc, sc_sql, loaded_inputs)
        else:
            output = self.etl_multi_pass(sc, sc_sql, loaded_inputs)
        return output

    def etl_multi_pass(self, sc, sc_sql, loaded_inputs={}):
        needs_run = True
        while needs_run:
            output = self.etl_one_pass(sc, sc_sql, loaded_inputs)
            if self.args.get('rerun_criteria') == 'last_date':
                needs_run = not self.final_inc
            elif self.args.get('rerun_criteria') == 'output_empty':
                needs_run = not self.output_empty
            elif self.args.get('rerun_criteria') == 'both':
                needs_run = not (self.output_empty or self.final_inc)
            if needs_run:
                del(output)
                gc.collect()
            logger.info('Incremental build needs other run -> {}'.format(needs_run))
        # TODO: check to change output to reload all outputs from inc build
        return output

    def etl_one_pass(self, sc, sc_sql, loaded_inputs={}):
        """ Main etl function, loads inputs, runs transform, and saves output."""
        logger.info("-------Starting running job '{}'--------".format(self.job_name))
        start_time = time()
        self.start_dt = datetime.utcnow() # attached to self so available within dev expose "transform()" func.
        output = self.etl_no_io(sc, sc_sql, loaded_inputs)
        logger.info('Output sample:')
        output.show()
        count = output.count()
        logger.info('Output count: {}'.format(count))
        logger.info("Output data types: {}".format([(fd.name, fd.dataType) for fd in output.schema.fields]))
        self.output_empty = count == 0
        if self.output_empty and self.is_incremental:
            logger.info("-------End job '{}', increment with empty output--------".format(self.job_name))
            # TODO: look at saving output empty table instead of skipping output.
            return output

        self.save(output, self.start_dt)
        end_time = time()
        elapsed = end_time - start_time
        logger.info('Process time to complete (post save to file but pre copy to db if any): {} s'.format(elapsed))
        # self.save_metadata(elapsed)  # disable for now to avoid spark parquet reading issues. TODO: check to re-enable.

        if self.redshift_copy_params:
            self.copy_to_redshift_using_spark(output)  # to use pandas: self.copy_to_redshift_using_pandas(output, self.OUTPUT_TYPES)
        if self.copy_to_kafka:
            self.push_to_kafka(output, self.OUTPUT_TYPES)

        output.unpersist()
        logger.info("-------End job '{}'--------".format(self.job_name))
        return output

    def etl_no_io(self, sc, sc_sql, loaded_inputs={}):
        """ Function to load inputs (including from live vars) and run transform. No output to disk.
        Having this code isolated is useful for cases with no I/O possible, like testing."""
        logger.info("etl args: {}".format(self.args))
        if self.args.get('mode_no_io'):  # TODO: check if condition can be skipped
            self.set_job_params(loaded_inputs)
        self.sc = sc
        self.sc_sql = sc_sql
        self.app_name = sc.appName
        self.logger = logger
        if self.job_name != self.app_name:
            logger.info("... part of spark app '{}'".format(self.app_name))

        loaded_datasets = self.load_inputs(loaded_inputs)
        output = self.transform(**loaded_datasets)
        output.cache()
        return output

    def transform(self, **app_args):
        """ The function that needs to be overriden by each specific job."""
        raise NotImplementedError

    def set_job_params(self, loaded_inputs={}, job_file=None):
        # TODO: redo without without the mapping.
        job_file = self.set_job_file() # file where code is, could be .py or .sql. ex "jobs/examples/ex1_frameworked_job.py" or "jobs/examples/ex1_full_sql_job.sql"
        jargs = Job_Args_Parser(cmd_args=self.args, job_file=job_file, get_all=True, loaded_inputs=loaded_inputs)  # has to be removed since already done in Commandliner()

        self.job_name = jargs.job_name  # name as written in jobs_metadata file, ex "examples/ex1_frameworked_job.py" or "examples/ex1_full_sql_job.sql"
        self.py_job = jargs.py_job  # name of python file supporting execution. Different from job_file for sql jobs or other generic python files.
        # self.app_name  # set earlier
        self.job_yml = jargs.yml_args
        self.INPUTS = jargs.inputs
        self.OUTPUT = jargs.output
        self.frequency = jargs.frequency
        self.redshift_copy_params = jargs.redshift_copy_params
        self.copy_to_kafka = jargs.copy_to_kafka
        self.db_creds = jargs.db_creds
        self.is_incremental = jargs.is_incremental

    def set_job_file(self):
        """ Returns the file being executed. For ex, when running "python some_job.py", this functions returns "some_job.py".
        Only to be used when the job is launched that way."""
        job_file = inspect.getsourcefile(self.__class__)
        logger.info("job_file: '{}'".format(job_file))
        return job_file

    def load_inputs(self, loaded_inputs):
        app_args = {}
        if self.db_creds:
            return app_args

        for item in self.INPUTS.keys():

            # Load from memory if available
            if item in loaded_inputs.keys():
                app_args[item] = loaded_inputs[item]
                logger.info("Input '{}' passed in memory from a previous job.".format(item))
                continue

            # Skip "other" types
            if self.INPUTS[item]['type'] == "other":
                app_args[item] = None
                logger.info("Input '{}' not loaded since type set to 'other'.".format(item))
                continue

            # Load from disk
            app_args[item] = self.load_input(item)
            logger.info("Input '{}' loaded.".format(item))

        if self.is_incremental:
            app_args = self.filter_incremental_inputs(app_args)

        self.sql_register(app_args)
        return app_args

    def filter_incremental_inputs(self, app_args):
        min_dt = self.get_previous_output_max_timestamp() if len(app_args.keys()) > 0 else None

        # Get latest timestamp in common across incremental inputs
        maxes = []
        for item in app_args.keys():
            input_is_tabular = self.INPUTS[item]['type'] in self.TABULAR_TYPES
            inc = self.INPUTS[item].get('inc_field', None)
            if input_is_tabular and inc:
                max_dt = app_args[item].agg({inc: "max"}).collect()[0][0]
                maxes.append(max_dt)
        max_dt = min(maxes) if len(maxes)>0 else None

        # Filter
        for item in app_args.keys():
            input_is_tabular = self.INPUTS[item]['type'] in self.TABULAR_TYPES
            inc = self.INPUTS[item].get('inc_field', None)
            if inc:
                if input_is_tabular:
                    # TODO: add limit to amount of input data, and set self.final_inc=False
                    inc_type = {k:v for k, v in app_args[item].dtypes}[inc]
                    logger.info("Input dataset '{}' will be filtered for min_dt={} max_dt={}".format(item, min_dt, max_dt))
                    if min_dt:
                        # min_dt = to_date(lit(s)).cast(TimestampType()  # TODO: deal with dt type, as coming from parquet
                        app_args[item] = app_args[item].filter(app_args[item][inc] > min_dt)
                    if max_dt:
                        app_args[item] = app_args[item].filter(app_args[item][inc] <= max_dt)
                else:
                    raise Exception("Incremental loading is not supported for unstructured input. You need to handle the incremental logic in the job code.")
        return app_args

    def sql_register(self, app_args):
        for item in app_args.keys():
            input_is_tabular = hasattr(app_args[item], "rdd")  # assuming DataFrame will keep 'rdd' attribute
            # ^ better than using self.INPUTS[item]['type'] in self.TABULAR_TYPES since doesn't require 'type' being defined.
            if input_is_tabular:
                app_args[item].createOrReplaceTempView(item)

    def load_input(self, input_name):
        input_type = self.INPUTS[input_name]['type']
        if input_type in self.FILE_TYPES:
            path = self.INPUTS[input_name]['path']
            path = path.replace('s3://', 's3a://') if self.args['mode'] == 'local' else path
            logger.info("Input '{}' to be loaded from files '{}'.".format(input_name, path))
            path = Path_Handler(path).expand_later(self.args['storage'])

        if input_type == 'txt':
            return self.sc.textFile(path)

        # Tabular types
        if input_type == 'csv':
            sdf = self.sc_sql.read.csv(path, header=True)  # TODO: add way to add .option("delimiter", ';'), useful for metric_budgeting.
        elif input_type == 'parquet':
            sdf = self.sc_sql.read.parquet(path)
        elif input_type == 'mysql':
            sdf = self.load_mysql(input_name)
        else:
            raise Exception("Unsupported input type '{}' for path '{}'. Supported types are: {}. ".format(input_type, self.INPUTS[input_name].get('path'), self.SUPPORTED_TYPES))

        logger.info("Input data types: {}".format([(fd.name, fd.dataType) for fd in sdf.schema.fields]))
        return sdf

    def load_mysql(self, input_name):
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.args['storage'], creds=self.args.get('connection_file'))
        creds_section = self.INPUTS[input_name]['creds']
        db = creds[creds_section]
        url = 'jdbc:mysql://{host}:{port}/{service}'.format(host=db['host'], port=db['port'], service=db['service'])
        dbtable = self.INPUTS[input_name]['db_table']
        logger.info('Pulling table "{}" from mysql'.format(dbtable))
        return self.sc_sql.read \
            .format('jdbc') \
            .option('driver', "com.mysql.jdbc.Driver") \
            .option("url", url) \
            .option("user", db['user']) \
            .option("password", db['password']) \
            .option("dbtable", dbtable)\
            .load()

    def get_previous_output_max_timestamp(self):
        path = self.OUTPUT['path']
        path += '*' # to go into subfolders
        try:
            df = self.load_input(path, self.OUTPUT['type'])
        except Exception as e:  # TODO: don't catch all
            logger.info("Previous increment could not be loaded or doesn't exist. It will be ignored. Folder '{}' failed loading with error '{}'.".format(path, e))
            return None

        dt = self.get_max_timestamp(df)
        logger.info("Max timestamp of previous increment: '{}'".format(dt))
        return dt

    def get_max_timestamp(self, df):
        return df.agg({self.OUTPUT['inc_field']: "max"}).collect()[0][0]

    def save(self, output, now_dt, path=None):
        if path is None:
            path = Path_Handler(self.OUTPUT['path']).expand_now(now_dt)
        self.path = path

        if self.OUTPUT['type'] == 'None':
            logger.info('Did not write output to disk')
            return None

        if self.is_incremental:
            current_time = now_dt.strftime('%Y%m%d_%H%M%S_utc')  # no use of now_dt to make it updated for each inc.
            file_tag = ('_' + self.args.get('file_tag')) if self.args.get('file_tag') else ""
            path += 'inc_{}{}/'.format(current_time, file_tag)

        # TODO: deal with cases where "output" is df when expecting rdd, or at least raise issue in a cleaner way.
        if self.OUTPUT['type'] == 'txt':
            output.saveAsTextFile(path)
        elif self.OUTPUT['type'] == 'parquet':
            output.write.parquet(path)
        elif self.OUTPUT['type'] == 'csv':
            output.write.option("header", "true").csv(path)
        else:
            raise Exception("Need to specify supported output type, either txt, parquet or csv.")

        logger.info('Wrote output to ' + path)

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
        logger.info('Query string:\n' + query_str)
        df =  self.sc_sql.sql(query_str)
        df.cache()
        return df

    def copy_to_redshift_using_pandas(self, output, types):
        # import put here below to avoid loading heavy libraries when not needed (optional feature).
        from core.redshift_pandas import create_table
        from core.db_utils import cast_col
        df = output.toPandas()
        df = cast_col(df, types)
        connection_profile = self.redshift_copy_params['creds']
        schema, name_tb= self.redshift_copy_params['table'].split('.')
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.args['storage'], creds=self.args.get('connection_file'))
        create_table(df, connection_profile, name_tb, schema, types, creds, self.is_incremental)
        del(df)

    def copy_to_redshift_using_spark(self, sdf):
        # import put here below to avoid loading heavy libraries when not needed (optional feature).
        from core.redshift_spark import create_table
        connection_profile = self.redshift_copy_params['creds']
        schema, name_tb= self.redshift_copy_params['table'].split('.')
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.args['storage'], creds=self.args.get('connection_file'))
        create_table(sdf, connection_profile, name_tb, schema, creds, self.is_incremental, REDSHIFT_S3_TMP_DIR)

    def push_to_kafka(self, output, types):
        """ Needs to be overriden by each specific job."""
        raise NotImplementedError


class Job_Args_Parser():
    def __init__(self, cmd_args={}, job_file=None, get_all=True, loaded_inputs={}):
        self.job_name, self.py_job, self.yml_args = self.set_job_main_params(cmd_args, job_file)
        if get_all:
            args = self.set_job_other_params(loaded_inputs, cmd_args, self.yml_args)
            [setattr(self, key, value) for key, value in args.items()]  # attach vars to self.*
            # TODO: check later that code below can be removed.
            # if cmd_args['storage'] == 's3' and job_file.startswith('jobs/'):
            #     # TODO: fix self.job_file, will break.
            #     self.job_file = CLUSTER_APP_FOLDER+job_file
            #     logger.info("overwrote job_file needed for running on cluster: '{}'".format(self.job_file))  # TODO: integrate this patch directly in var assignment above when refactored, to avoid conflicting messages

    def set_job_main_params(self, cmd_args, job_file=None):
        job_name = cmd_args.get('job_name')
        if job_name:  # job_name (name from job_metadata.yml) takes priority if provided.
            yml_args = self.set_job_yml(cmd_args, job_name)
            py_job = yml_args['py_job'] if yml_args.get('py_job') else self.set_job_file_from_name(job_name)
        elif job_file:
            py_job = job_file
            job_name = self.set_job_name_from_file(job_file)
            yml_args = self.set_job_yml(cmd_args, job_name)
        else:
            raise Exception("Need to specify at least job_name or job_file")

        return job_name, py_job, yml_args

    def set_job_other_params(self, loaded_inputs, cmd_args, yml_args):
        """ Setting the params from yml or from commandline args if available."""  # TODO: change so class doesn't involve commandline args here, just yml.
        args = {}
        args['inputs'] = self.set_inputs(cmd_args, yml_args, loaded_inputs)
        args['output'] = self.set_output(cmd_args, yml_args)
        args['frequency'] = self.set_generic_param(cmd_args, yml_args, param='frequency')
        args['start_date'] = self.set_generic_param(cmd_args, yml_args, param='start_date')
        args['is_incremental'] = self.set_is_incremental(args['inputs'], args['output'])
        args['db_creds'] = self.set_db_creds(cmd_args, yml_args)
        args['redshift_copy_params'] = self.set_generic_param(cmd_args, yml_args, param='copy_to_redshift')
        args['copy_to_kafka'] = self.set_generic_param(cmd_args, yml_args, param='copy_to_kafka')
        return args

    def set_job_name_from_file(self, job_file):
        # when run from Flow(), job_file is full path. When run from ETL directly, job_file is "jobs/..." .
        if job_file.startswith(CLUSTER_APP_FOLDER+'jobs/'):
            job_name = job_file[len(CLUSTER_APP_FOLDER+'jobs/'):]
        elif job_file.startswith(CLUSTER_APP_FOLDER+'scripts.zip/jobs/'):
            job_name = job_file[len(CLUSTER_APP_FOLDER+'scripts.zip/jobs/'):]
        elif job_file.startswith(LOCAL_APP_FOLDER+'jobs/'):
            job_name = job_file[len(LOCAL_APP_FOLDER+'jobs/'):]
        elif job_file.startswith(LOCAL_JOB_REPO_FOLDER+'jobs/'):  # when run from external repo.
            job_name = job_file[len(LOCAL_JOB_REPO_FOLDER+'jobs/'):]
        elif job_file.startswith('jobs/'):
            job_name = job_file[len('jobs/'):]
        elif job_file.__contains__('/scripts.zip/jobs/'):
            # To deal with cases like job_file = '/mnt/tmp/spark-48e465ad-cca8-4216-a77f-ce069d04766f/userFiles-b1dad8aa-76ea-4adf-97da-dc9273666263/scripts.zip/jobs/infojobs/churn_prediction/users_inscriptions_daily.py' that appeared in new emr version.
            job_name = job_file[job_file.find('/scripts.zip/jobs/')+len('/scripts.zip/jobs/'):]
        else:
            # To deal with case when job is defined outside of this repo (and not in jobs/ folder in external folder), i.e. isn't located in 'jobs/' folder. In this case, job name in metadata file should include full path (inc job base path).
            job_name = job_file
        logger.info("job_name: '{}', from job_file: '{}'".format(job_name, job_file))
        return job_name

    def set_job_file_from_name(self, job_name):
        job_file='jobs/{}'.format(job_name)
        logger.info("job_name: '{}', and corresponding job_file: '{}'".format(job_name, job_file))
        return job_file

    def set_job_yml(self, cmd_args, job_name):
        meta_file = cmd_args.get('job_param_file')
        if meta_file == 'repo':
            meta_file = CLUSTER_APP_FOLDER+JOBS_METADATA_FILE if cmd_args['storage']=='s3' else JOBS_METADATA_LOCAL_FILE
        elif meta_file is None:
            return None

        yml = self.load_meta(meta_file)
        logger.info('Loaded job param file: ' + meta_file)

        try:
            return yml[job_name]
        except KeyError:
            raise KeyError("Your job '{}' can't be found in jobs_metadata file '{}'. Add it there or make sure the name matches".format(job_name, meta_file))

    def set_generic_param(self, cmd_args, yml_args, param):
        if cmd_args.get(param):
            return cmd_args[param]
        elif cmd_args.get('job_param_file'):
            return yml_args.get(param)
        else:
            return None

    def set_inputs(self, cmd_args, yml_args, loaded_inputs):
        inputs_in_args = len([item for item in cmd_args.keys() if item.startswith('input_')]) >= 1
        if inputs_in_args:
            return {key.replace('input_', ''): {'path': val, 'type': 'df'} for key, val in cmd_args.items() if key.startswith('input_')}
        elif cmd_args.get('job_param_file'):  # should be before loaded_inputs to use yaml if available. Later function load_inputs uses both self.INPUTS and loaded_inputs, so not incompatible.
            return yml_args.get('inputs') or {}
        elif loaded_inputs:
            return {key: {'path': val, 'type': 'df'} for key, val in loaded_inputs.items()}
        else:
            logger.info("No input given, through commandline nor yml file.")
            return {}

    def set_output(self, cmd_args, yml_args):
        output = self.set_generic_param(cmd_args, yml_args, param='output')
        if output is None and cmd_args.get('mode_no_io'):
            output = {}
            logger.info("No output given")
        elif output is None:
            raise Exception("No output given")
        logger.info("output: '{}'".format(output))
        return output

    def set_db_creds(self, cmd_args, yml_args):
        if cmd_args.get('db_creds'):
            return cmd_args['db_creds']
        elif cmd_args.get('job_param_file') and yml_args.get('from_redshift'):
            return yml_args['from_redshift'].get('creds')
        else:
            return None

    def set_is_incremental(self, inputs, output):
        return any(['inc_field' in inputs[item] for item in inputs.keys()]) or 'inc_field' in output

    @staticmethod
    def load_meta(fname):
        with open(fname, 'r') as stream:
            yml = yaml.load(stream)
        return yml

    # def update_args(self, args, job_file):
    #     if job_file.endswith('.sql'):
    #         args['sql_file'] = job_file
    #     return args


class FS_Ops_Dispatcher():
    # --- save_metadata set of functions ----
    def save_metadata(self, fname, content, storage):
        self.save_metadata_cluster(fname, content) if storage=='s3' else self.save_metadata_local(fname, content)

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
        s3c = boto3.client('s3')
        s3c.put_object(Bucket=bucket_name, Key=bucket_fname, Body=fake_handle.read())
        logger.info("Created file S3: {}".format(fname))

    # --- save_file set of functions ----
    def save_file(self, fname, content, storage):
        self.save_file_cluster(fname, content) if storage=='s3' else self.save_file_local(fname, content)

    @staticmethod
    def save_file_local(fname, content):
        folder = os.path.dirname(fname)
        if not os.path.exists(folder):
            os.makedirs(folder)
        joblib.dump(content, fname)
        logger.info("Saved content to new file locally: {}".format(fname))

    def save_file_cluster(self, fname, content):
        fname_parts = fname.split('s3://')[1].split('/')
        bucket_name = fname_parts[0]
        bucket_fname = '/'.join(fname_parts[1:])
        s3c = boto3.client('s3')

        local_path = CLUSTER_APP_FOLDER+'tmp/local_'+fname_parts[-1]
        self.save_file_local(local_path, content)
        fh = open(local_path, 'rb')
        s3c.put_object(Bucket=bucket_name, Key=bucket_fname, Body=fh)
        logger.info("Pushed local file to S3, from '{}' to '{}' ".format(local_path, fname))

    # --- load_file set of functions ----
    def load_file(self, fname, storage):
        return self.load_file_cluster(fname) if storage=='s3' else self.load_file_local(fname)

    @staticmethod
    def load_file_local(fname):
        return joblib.load(fname)

    @staticmethod
    def load_file_cluster(fname):
        fname_parts = fname.split('s3://')[1].split('/')
        bucket_name = fname_parts[0]
        bucket_fname = '/'.join(fname_parts[1:])
        local_path = CLUSTER_APP_FOLDER+'tmp/s3_'+fname_parts[-1]
        s3c = boto3.client('s3')
        s3c.download_file(bucket_name, bucket_fname, local_path)
        logger.info("Copied file from S3 '{}' to local '{}'".format(fname, local_path))
        model = joblib.load(local_path)
        return model

    # --- listdir set of functions ----
    def listdir(self, path, storage):
        return self.listdir_cluster(path) if storage=='s3' else self.listdir_local(path)

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
        client = boto3.client('s3')
        objects = client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        paths = [item['Prefix'].split('/')[-2] for item in objects.get('CommonPrefixes')]
        return paths

    # --- dir_exist set of functions ----
    def dir_exist(self, path, storage):
        return self.dir_exist_cluster(path) if storage=='s3' else self.dir_exist_local(path)

    @staticmethod
    def dir_exist_local(path):
        return os.path.isdir(path)

    @staticmethod
    def dir_exist_cluster(path):
        raise Exception("Not implemented")


class Cred_Ops_Dispatcher():
    def retrieve_secrets(self, storage, creds='conf/connections.cfg'):
        creds = self.retrieve_secrets_cluster() if storage=='s3' else self.retrieve_secrets_local(creds)
        return creds

    @staticmethod
    def retrieve_secrets_cluster():
        client = boto3.client('secretsmanager')

        response = client.get_secret_value(SecretId=AWS_SECRET_ID)
        logger.info('Read aws secret, secret_id:'+AWS_SECRET_ID)
        logger.debug('get_secret_value response: '+str(response))
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

    def expand_now(self, now_dt):
        path = self.path
        if '{now}' in path:
            current_time = now_dt.strftime('date%Y%m%d_time%H%M%S_utc')
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
        self.set_commandline_args(args)  # sets self.args TODO: make explicit
        if Job is None:  # when job run from launcher.py --job_name=some_name_from_job_metadata_file
            assert self.args['job_name']
            jargs = Job_Args_Parser(cmd_args=self.args, job_file=None, get_all=False)
            Job = get_job_class(jargs.py_job)

        if self.args['mode'] in ('local', 'localEMR'):
            self.launch_run_mode(Job, self.args)
        else:  # when deploying to AWS
            job = Job(self.args)
            job_file = job.set_job_file()
            jargs = Job_Args_Parser(cmd_args=self.args, job_file=job_file, get_all=True)
            deploy_args = {'aws_config_file': self.args.pop('aws_config_file'),
                           'aws_setup': self.args.pop('aws_setup'),
                           'leave_on': self.args.pop('leave_on'),
                           }
            self.launch_deploy_mode(jargs, deploy_args, app_args=self.args)  # TODO: make deployment args explicit + preprocess yml param upstread and remove it here.

    def set_commandline_args(self, args):
        """Command line arguments take precedence over function ones."""
        parser, defaults = self.define_commandline_args()
        cmd_args, unknown_args = parser.parse_known_args()
        cmd_args = {key: value for (key, value) in cmd_args.__dict__.items() if value is not None}
        unknown_args = dict([item[2:].split('=') for item in unknown_args])  # imposes for unknown args to be defined with '=' and to start with '--'

        #load defaults, overwrite by yml, overwrite job commandliner(), overwrite by cmdline args if any.
        self.args = defaults
        self.args.update(args)  # same
        self.args.update(cmd_args)  # cmd_args (if set) overwrite upstream (function defined) args
        self.args.update(unknown_args)  # same

    @staticmethod
    def define_commandline_args():
        # Defined here separatly for overridability.
        parser = argparse.ArgumentParser()
        parser.add_argument("-m", "--mode", choices=set(['local', 'EMR', 'localEMR', 'EMR_Scheduled', 'EMR_DataPipeTest']), help="Choose where to run the job. localEMR should not be used by user.")
        parser.add_argument("-j", "--job_param_file", help="Identify file to use. If 'repo', then default files from the repo are used. It can be set to 'False' to not load any file and provide all parameters through arguments.")
        parser.add_argument("-n", "--job_name", help="Identify registry job to use.")
        parser.add_argument("--connection_file", help="Identify file to use. Default to repo one.")
        parser.add_argument("--jobs_folder", help="Identify the folder where job code is. Necessary if job code is outside the repo, i.e. if this is used as an external library. By default, uses the repo 'jobs/' folder.")
        parser.add_argument("-s", "--storage", choices=set(['local', 's3']), help="Choose 'local' (default) or 's3'.")
        parser.add_argument("-x", "--dependencies", action='store_true', help="Run the job dependencies and then the job itself")
        parser.add_argument("-c", "--rerun_criteria", choices=set(['last_date', 'output_empty', 'both']), help="Choose criteria to rerun the next increment or not. 'last_date' usefull if we know data goes to a certain date. 'output_empty' not to be used if increment may be empty but later ones not. Only relevant for incremental job.")
        parser.add_argument("-b", "--boxed_dependencies", action='store_true', help="Run dependant jobs in a sandboxed way, i.e. without passing output to next step. Only useful if ran with dependencies (-x).")
        parser.add_argument("-l", "--load_connectors", choices=set(['all', 'none']), help="Load java packages to enable spark connectors (s3, redshift, mysql). Set to 'none' to have faster spark start time and smaller log when connectors are not necessary. Only useful if running in --mode=local.")
        # Deploy specific
        parser.add_argument("--aws_config_file", help="Identify file to use. Default to repo one.")
        parser.add_argument("-a", "--aws_setup", help="Choose aws setup from conf/aws_config.cfg, typically 'prod' or 'dev'. Only relevant if choosing to deploy to a cluster.")
        parser.add_argument("-o", "--leave_on", action='store_true', help="Use arg to not terminate cluster after running the job. Mostly for testing. Only relevant when creating a new cluster in mode 'EMR'.")
        parser.add_argument("-p", "--push_secrets", action='store_true', help="Pushing secrets to cluster. Only relevant if choosing to deploy to a cluster.")
        # parser.add_argument("-o", "--output", default=None, help="output path")
        # For later : --machines, --inputs, to be integrated only as a way to overide values from file.
        defaults = {
                    'mode': 'local',
                    'job_param_file': 'repo',
                    'job_name': None,
                    'connection_file': CONNECTION_FILE,
                    'jobs_folder': JOB_FOLDER,
                    'storage': 'local',
                    # 'dependencies': False, # only set from commandline
                    'rerun_criteria': 'both',
                    # 'boxed_dependencies': False,  # only set from commandline
                    'load_connectors': 'all',
                    # Deploy specific below
                    'aws_config_file': AWS_CONFIG_FILE,
                    'aws_setup': 'dev',
                    # 'leave_on': False, # only set from commandline
                    # 'push_secrets': False, # only set from commandline
                    }
        return parser, defaults

    def launch_run_mode(self, Job, args):
        job = Job(args)
        job.set_job_params() # just need the job.job_name from there.
        app_name = job.job_name
        sc, sc_sql = self.create_contexts(app_name, args['mode'], args['load_connectors'])
        if not self.args['dependencies']:
            job.etl(sc, sc_sql)
        else:
            Flow(sc, sc_sql, args, app_name)

    def launch_deploy_mode(self, yml, deploy_args, app_args):
        # Load deploy lib here instead of at module level to remove dependency on it when running code locally
        from core.deploy import DeployPySparkScriptOnAws
        DeployPySparkScriptOnAws(yml, deploy_args, app_args).run()

    def create_contexts(self, app_name, mode, load_connectors):
        # Load spark here instead of at module level to remove dependency on spark when only deploying code to aws.
        from pyspark.sql import SQLContext
        from pyspark.sql import SparkSession
        from pyspark import SparkConf

        if mode == 'local' and load_connectors == 'all':
            # S3 access
            session = boto3.Session()
            credentials = session.get_credentials()
            os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
            # JARs
            conf = SparkConf() \
                .set("spark.jars.packages", PACKAGES_LOCAL) \
                .set("spark.jars", JARS)
        else:
            conf = SparkConf()

        spark = SparkSession.builder \
            .appName(app_name) \
            .config(conf=conf) \
            .getOrCreate()
        sc = spark.sparkContext

        ## Other ways to change spark config from python, instead of using spark-defaults.conf or adding jars in spark-submit
        # os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars extra/ojdbc6.jar'  # or s3://some_path/ojdbc6.jar
        # sc._jsc.addJAR(/path/to.jar)
        # sc.addFile("filename") # to add a file in every nodes.
        ## Hadoop configuration
        # sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", credentials.access_key)  # doesn't work.
        # sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", credentials.secret_key)  # doesn't work.
        # or use env variable
        # export AWS_ACCESS_KEY_ID=`aws configure get default.aws_access_key_id`
        # export AWS_SECRET_ACCESS_KEY=`aws configure get default.aws_secret_access_key`
        ## Other
        # sc.setLogLevel("ERROR")

        sc_sql = SQLContext(sc)
        logger.info('Spark Config: {}'.format(sc.getConf().getAll()))
        return sc, sc_sql


class Flow():
    def __init__(self, sc, sc_sql, args, app_name):
        self.app_name = app_name
        storage = args['storage']
        df = self.create_connections_jobs(storage, args)
        logger.debug('Flow app_name : {}, connection_table: {}'.format(app_name, df))
        graph = self.create_global_graph(df)  # top to bottom
        tree = self.create_local_tree(graph, nx.DiGraph(), app_name) # bottom to top
        leafs = self.get_leafs(tree, leafs=[]) # bottom to top
        logger.info('Sequence of jobs to be run: {}'.format(leafs))
        logger.info('-'*80)
        logger.info('-')

        # load all job classes and run them
        df = {}
        for job_name in leafs:
            args['job_name'] = job_name
            jargs = Job_Args_Parser(cmd_args=args, job_file=None, get_all=False)
            # args = jargs.update_args(args, jargs.job_file)
            Job = get_job_class(jargs.py_job)
            job = Job(args)
            logger.info('About to run : {}'.format(job_name))

            loaded_inputs = {}
            if not args['boxed_dependencies']:
                if jargs.job_yml.get('inputs', 'no input') == 'no input':
                    raise Exception("Pb with loading job_yml or finding 'inputs' parameter in it. You can work around it by using 'boxed_dependencies' argument.")
                for in_name, in_properties in jargs.job_yml['inputs'].items():
                    if in_properties.get('from'):
                        loaded_inputs[in_name] = df[in_properties['from']]
            df[job_name] = job.etl(sc, sc_sql, loaded_inputs) # at this point df[job_name] is unpersisted.
            if args['boxed_dependencies']:
                df[job_name].unpersist()
                del df[job_name]
                gc.collect()
            logger.info('-'*80)
            logger.info('-')

    def create_connections_jobs(self, storage, args):
        meta_file = args.get('job_param_file')
        if meta_file == 'repo':
            meta_file = CLUSTER_APP_FOLDER+JOBS_METADATA_FILE if args['storage']=='s3' else JOBS_METADATA_LOCAL_FILE
        yml = Job_Args_Parser.load_meta(meta_file)

        connections = []
        for job_name, job_meta in yml.items():
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
            DG.add_node(source_dataset, name=source_dataset) # (source_dataset, **{'name':source_dataset})
            DG.add_node(target_dataset, **item)
        return DG

    def create_local_tree(self, DG, tree, ref_node):
        """ Builds tree recursively. Uses graph data structure but enforces tree to simplify downstream."""
        nodes = DG.predecessors(ref_node)
        tree.add_node(ref_node, name=DG.nodes[ref_node])
        for item in nodes:
            if not tree.has_node(item):
                tree.add_edge(ref_node, item)
                tree.add_node(item, name=DG.nodes[item])
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
        return leafs + list(tree.nodes())

def get_job_class(py_job):
    name_import = py_job.replace('/','.').replace('.py','')
    import_cmd = "from {} import Job".format(name_import)
    namespace = {}
    exec(import_cmd, namespace)
    return namespace['Job']
