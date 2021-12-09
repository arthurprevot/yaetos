"""
Helper functions. Setup to run locally and on cluster.
"""
# TODO:
# - add linter
# - finish _metadata.txt file content.
# - get inputs and output by commandline (with all related params used in yml, like 'type', 'incr'...).
# - better check that db copy is in sync with S3.
# - way to run all jobs from 1 cmd line.
# - make boxed_dependencies the default, as more conservative.


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
from pprint import pformat
import smtplib, ssl
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from yaetos.git_utils import Git_Config_Manager
from dateutil.relativedelta import relativedelta
from yaetos.logger import setup_logging
logger = setup_logging('Job')


# User settable params below can be changed from command line or yml or job inputs.
JOBS_METADATA_FILE = 'conf/jobs_metadata.yml'
AWS_CONFIG_FILE = 'conf/aws_config.cfg'
CONNECTION_FILE = 'conf/connections.cfg'
CLUSTER_APP_FOLDER = '/home/hadoop/app/'
LOCAL_APP_FOLDER = os.environ.get('PYSPARK_AWS_ETL_HOME', '') # PYSPARK_AWS_ETL_HOME set to end with '/'
LOCAL_JOB_REPO_FOLDER = os.environ.get('PYSPARK_AWS_ETL_JOBS_HOME', '')
AWS_SECRET_ID = '/yaetos/connections'
JOB_FOLDER = 'jobs/'
PACKAGES_EMR = ['com.databricks:spark-redshift_2.11:2.0.1', 'org.apache.spark:spark-avro_2.11:2.4.0', 'mysql:mysql-connector-java:8.0.22', 'org.postgresql:postgresql:42.2.18']  # necessary for reading/writing to redshift, mysql & clickhouse using spark connector.
PACKAGES_EMR_ALT = ['io.github.spark-redshift-community:spark-redshift_2.12:5.0.3', 'org.apache.spark:spark-avro_2.12:3.1.1', 'mysql:mysql-connector-java:8.0.22', 'org.postgresql:postgresql:42.2.18']  # same but compatible with spark 3.
PACKAGES_LOCAL = PACKAGES_EMR + ['com.amazonaws:aws-java-sdk-pom:1.11.760', 'org.apache.hadoop:hadoop-aws:2.7.0']
PACKAGES_LOCAL_ALT = PACKAGES_EMR_ALT + ['com.amazonaws:aws-java-sdk-pom:1.11.760', 'org.apache.hadoop:hadoop-aws:2.7.0']  # will probably need to be moved to hadoop-aws:3.2.1 to work locally.
JARS = 'https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.41.1065/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar'  # not available in public repo so cannot be put in "packages" var.


class ETL_Base(object):
    TABULAR_TYPES = ('csv', 'parquet', 'df', 'mysql', 'clickhouse')
    FILE_TYPES = ('csv', 'parquet', 'txt')
    SUPPORTED_TYPES = set(TABULAR_TYPES).union(set(FILE_TYPES)).union({'other', 'None'})

    def __init__(self, pre_jargs={}, jargs=None, loaded_inputs={}):
        self.loaded_inputs = loaded_inputs
        self.jargs = self.set_jargs(pre_jargs, loaded_inputs) if not jargs else jargs
        if self.jargs.manage_git_info:
            git_yml = Git_Config_Manager().get_config(mode=self.jargs.mode, local_app_folder=LOCAL_APP_FOLDER, cluster_app_folder=CLUSTER_APP_FOLDER)
            [git_yml.pop(key, None) for key in ('diffs_current', 'diffs_yaetos') if git_yml]
            logger.info('Git info {}'.format(git_yml))

    def etl(self, sc, sc_sql):
        """ Main function. If incremental, reruns ETL process multiple time until
        fully loaded, otherwise, just run ETL once.
        It's a way to deal with case where full incremental rerun from scratch would
        require a larger cluster to build in 1 shot than the typical incremental.
        """
        try:
            if not self.jargs.is_incremental:
                output = self.etl_one_pass(sc, sc_sql, self.loaded_inputs)
            else:
                output = self.etl_multi_pass(sc, sc_sql, self.loaded_inputs)
        except Exception as err:
            if self.jargs.mode in ('prod_EMR') and self.jargs.merged_args.get('owners'):
                self.send_job_failure_email(err)
            raise Exception("Job failed, error: \n{}".format(err))
        return output

    def etl_multi_pass(self, sc, sc_sql, loaded_inputs={}):
        needs_run = True
        ii = 0
        while needs_run:  # TODO: check to rewrite as for loop. Simpler and avoiding potential infinite loops.
            # TODO: isolate code below into separate functions.
            ii+=1
            if self.jargs.merged_args.get('job_increment') == 'daily':
                if ii == 1:
                    first_day = self.jargs.merged_args['first_day']
                    last_run_period = self.get_last_run_period_daily(sc, sc_sql)
                    periods = Period_Builder().get_last_output_to_last_day(last_run_period, first_day)

                if len(periods) == 0:
                    logger.info('Output up to date. Nothing to run. last processed period={} and last period from now={}'.format(last_run_period, Period_Builder.get_last_day()))
                    output = sc_sql.createDataFrame([], StructType([]))
                    self.final_inc = True  # remove "self." when sandbox job doesn't depend on it.
                else:
                    logger.info('Periods remaining to load: {}'.format(periods))
                    period = periods[0]
                    logger.info('Period to be loaded in this run: {}'.format(period))
                    self.period = period  # to be captured in etl_one_pass, needed for in database filtering.
                    self.period_next = periods[1] if len(periods)>=2 else None  # same
                    self.jargs.merged_args['file_tag'] = period
                    output = self.etl_one_pass(sc, sc_sql, loaded_inputs)
                    self.final_inc = period == periods[-1]
                    periods.pop(0)  # for next increment.
            else:
                raise Exception("'job_increment' param has to be set to 'daily'")

            if self.jargs.rerun_criteria == 'last_date':  # i.e. stop when reached final increment, i.e. current period is last to process. Pb: can go in infinite loop if missing data.
                needs_run = not self.final_inc
            elif self.jargs.rerun_criteria == 'output_empty':  # i.e. stop when current inc is empty. Good to deal with late arriving data, but will be a pb if some increment doesn't have data and will never have.
                needs_run = not self.output_empty
            elif self.jargs.rerun_criteria == 'both':
                needs_run = not (self.output_empty or self.final_inc)
            if needs_run:
                del(output)
                gc.collect()
            logger.info('Incremental build needs other run -> {}'.format(needs_run))
        # TODO: check to change output to reload all outputs from inc build
        return output

    def etl_one_pass(self, sc, sc_sql, loaded_inputs={}):
        """ Main etl function, loads inputs, runs transform, and saves output."""
        logger.info("-------Starting running job '{}'--------".format(self.jargs.job_name))
        start_time = time()
        self.start_dt = datetime.utcnow() # attached to self so available within "transform()" func.
        output, schemas = self.etl_no_io(sc, sc_sql, loaded_inputs)
        if output is None:
            if self.jargs.is_incremental:
                logger.info("-------End job '{}', increment with empty output--------".format(self.jargs.job_name))
                self.output_empty = True
            else:
                logger.info("-------End job '{}', no output--------".format(self.jargs.job_name))
            # TODO: add process time in that case.
            return None

        if not self.jargs.no_fw_cache or (self.jargs.is_incremental and self.jargs.rerun_criteria == 'output_empty'):
            logger.info('Output sample:')
            try:
                output.show()
            except Exception as e:
                logger.info("Warning: Failed showing table sample with error '{}'.".format(e))
                pass
            count = output.count()
            logger.info('Output count: {}'.format(count))
            logger.info("Output data types: {}".format(pformat([(fd.name, fd.dataType) for fd in output.schema.fields])))
            self.output_empty = count == 0

        self.save_output(output, self.start_dt)
        end_time = time()
        elapsed = end_time - start_time
        logger.info('Process time to complete (post save to file but pre copy to db if any, also may not include processing if output not saved): {} s'.format(elapsed))
        if self.jargs.save_schemas and schemas:
            schemas.save_yaml(self.jargs.job_name)
        # self.save_metadata(elapsed)  # disable for now to avoid spark parquet reading issues. TODO: check to re-enable.

        if self.jargs.merged_args.get('copy_to_redshift') and self.jargs.enable_redshift_push:
            self.copy_to_redshift_using_spark(output)  # to use pandas: self.copy_to_redshift_using_pandas(output, self.OUTPUT_TYPES)
        if self.jargs.merged_args.get('copy_to_clickhouse') and self.jargs.enable_redshift_push:  # TODO: rename enable_redshift_push to enable_db_push since not redshift here.
            self.copy_to_clickhouse(output)
        if self.jargs.merged_args.get('copy_to_kafka'):
            self.push_to_kafka(output, self.OUTPUT_TYPES)

        output.unpersist()
        end_time = time()
        elapsed = end_time - start_time
        logger.info('Process time to complete job (post db copies if any): {} s'.format(elapsed))
        logger.info("-------End job '{}'--------".format(self.jargs.job_name))
        return output

    def etl_no_io(self, sc, sc_sql, loaded_inputs={}, jargs=None):
        """ Function to load inputs (including from live vars) and run transform. No output to disk.
        Having this code isolated is useful for cases with no I/O possible, like testing."""
        self.jargs = jargs or self.jargs
        self.sc = sc
        self.sc_sql = sc_sql
        self.app_name = sc.appName
        self.logger = logger
        if self.jargs.job_name != self.app_name:
            logger.info("... part of spark app '{}'".format(self.app_name))

        loaded_datasets = self.load_inputs(loaded_inputs)
        output = self.transform(**loaded_datasets)
        if output and self.jargs.output['type'] in self.TABULAR_TYPES:
            if self.jargs.add_created_at=='true':
                output = output.withColumn('_created_at', F.lit(self.start_dt))
            output.cache()
            schemas = Schema_Builder()
            schemas.generate_schemas(loaded_datasets, output)
        else:
            schemas = None
        return output, schemas

    def etl_no_io_function(self, sc, sc_sql, transform=None, transform_args={}, loaded_inputs={}):
        """ Used for testing internal functions"""
        # self.jargs = jargs
        self.sc = sc
        self.sc_sql = sc_sql
        self.app_name = sc.appName
        self.logger = logger
        loaded_datasets = self.load_inputs(loaded_inputs)
        output = transform(**transform_args)
        return output

    def transform(self, **app_args):
        """ The function that needs to be overriden by each specific job."""
        raise NotImplementedError

    def get_last_run_period_daily(self, sc, sc_sql):
        previous_output_max_timestamp = self.get_previous_output_max_timestamp(sc, sc_sql)
        last_run_period  = previous_output_max_timestamp.strftime("%Y-%m-%d") if previous_output_max_timestamp else None  # TODO: if get_output_max_timestamp()=None, means new build, so should delete instance in DBs.
        return last_run_period

    def set_jargs(self, pre_jargs, loaded_inputs={}):
        """ jargs means job args. Function called only if running the job directly, i.e. "python some_job.py"""
        py_job = self.set_py_job()
        job_name = Job_Yml_Parser.set_job_name_from_file(py_job)
        return Job_Args_Parser(defaults_args=pre_jargs['defaults_args'], yml_args=None, job_args=pre_jargs['job_args'], cmd_args=pre_jargs['cmd_args'], job_name=job_name, loaded_inputs=loaded_inputs)  # set yml_args=None so loading yml is handled in Job_Args_Parser()

    def set_py_job(self):
        """ Returns the file being executed. For ex, when running "python some_job.py", this functions returns "some_job.py".
        Only gives good output when the job is launched that way."""
        py_job = inspect.getsourcefile(self.__class__)
        logger.info("py_job: '{}'".format(py_job))
        return py_job

    def load_inputs(self, loaded_inputs):
        app_args = {}
        for item in self.jargs.inputs.keys():

            # Load from memory if available
            if item in loaded_inputs.keys():
                app_args[item] = loaded_inputs[item]
                logger.info("Input '{}' passed in memory from a previous job.".format(item))
                continue

            # Skip "other" types
            if self.jargs.inputs[item]['type'] == "other":
                app_args[item] = None
                logger.info("Input '{}' not loaded since type set to 'other'.".format(item))
                continue

            # Load from disk
            app_args[item] = self.load_input(item)
            logger.info("Input '{}' loaded.".format(item))

        if self.jargs.is_incremental and self.jargs.inputs[item]['type'] not in ('mysql', 'clickouse'):
            if self.jargs.merged_args.get('motm_incremental'):
                app_args = self.filter_incremental_inputs_motm(app_args)
            else:
                app_args = self.filter_incremental_inputs_period(app_args)

        self.sql_register(app_args)
        return app_args

    def filter_incremental_inputs_motm(self, app_args):
        """Filter based on Min Of The Max (motm) of all inputs. Good to deal with late arriving data or async load but
        gets stuck if 1 input never has any new data arriving.
        Assumes increment fields are datetime."""
        min_dt = self.get_previous_output_max_timestamp(self.sc, self.sc_sql) if len(app_args.keys()) > 0 else None

        # Get latest timestamp in common across incremental inputs
        maxes = []
        for item in app_args.keys():
            input_is_tabular = self.jargs.inputs[item]['type'] in self.TABULAR_TYPES
            inc = self.jargs.inputs[item].get('inc_field', None)
            if input_is_tabular and inc:
                max_dt = app_args[item].agg({inc: "max"}).collect()[0][0]
                maxes.append(max_dt)
        max_dt = min(maxes) if len(maxes)>0 else None

        # Filter
        for item in app_args.keys():
            input_is_tabular = self.jargs.inputs[item]['type'] in self.TABULAR_TYPES
            inc = self.jargs.inputs[item].get('inc_field', None)
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

    def filter_incremental_inputs_period(self, app_args):
        """Filter based on period defined in. Simple but can be a pb if late arriving data or dependencies not run.
        Inputs filtered inside source database will be filtered again."""
        for item in app_args.keys():
            input_is_tabular = self.jargs.inputs[item]['type'] in self.TABULAR_TYPES
            inc = self.jargs.inputs[item].get('inc_field', None)
            if inc:
                if input_is_tabular:
                    # TODO: add limit to amount of input data, and set self.final_inc=False
                    inc_type = {k:v for k, v in app_args[item].dtypes}[inc]
                    logger.info("Input dataset '{}' will be filtered for {}='{}'".format(item, inc, self.period))
                    app_args[item] = app_args[item].filter(app_args[item][inc] == self.period)
                else:
                    raise Exception("Incremental loading is not supported for unstructured input. You need to handle the incremental logic in the job code.")
        return app_args

    def sql_register(self, app_args):
        for item in app_args.keys():
            input_is_tabular = hasattr(app_args[item], "rdd")  # assuming DataFrame will keep 'rdd' attribute
            # ^ better than using self.jargs.inputs[item]['type'] in self.TABULAR_TYPES since doesn't require 'type' being defined.
            if input_is_tabular:
                app_args[item].createOrReplaceTempView(item)

    def load_input(self, input_name):
        input_type = self.jargs.inputs[input_name]['type']
        if input_type in self.FILE_TYPES:
            path = self.jargs.inputs[input_name]['path']
            path = path.replace('s3://', 's3a://') if self.jargs.mode == 'dev_local' else path
            logger.info("Input '{}' to be loaded from files '{}'.".format(input_name, path))
            path = Path_Handler(path, self.jargs.base_path).expand_later(self.jargs.storage)

        if input_type == 'txt':
            rdd = self.sc.textFile(path)
            logger.info("Input '{}' loaded from files '{}'.".format(input_name, path))
            return rdd

        # Tabular types
        if input_type == 'csv':
            delimiter = self.jargs.merged_args.get('csv_delimiter', ',')
            sdf = self.sc_sql.read.option("delimiter", delimiter).csv(path, header=True)
            logger.info("Input '{}' loaded from files '{}'.".format(input_name, path))
        elif input_type == 'parquet':
            sdf = self.sc_sql.read.parquet(path)
            logger.info("Input '{}' loaded from files '{}'.".format(input_name, path))
        elif input_type == 'mysql':
            sdf = self.load_mysql(input_name)
            logger.info("Input '{}' loaded from mysql".format(input_name))
        elif input_type == 'clickhouse':
            sdf = self.load_clickhouse(input_name)
            logger.info("Input '{}' loaded from clickhouse".format(input_name))
        else:
            raise Exception("Unsupported input type '{}' for path '{}'. Supported types are: {}. ".format(input_type, self.jargs.inputs[input_name].get('path'), self.SUPPORTED_TYPES))

        logger.info("Input data types: {}".format(pformat([(fd.name, fd.dataType) for fd in sdf.schema.fields])))
        return sdf

    def load_data_from_files(self, name, path, type, sc, sc_sql):
        """Loading any dataset (input or not) and only from file system (not from DBs). Used by incremental jobs to load previous output.
        Different from load_input() which only loads input (input jargs hardcoded) and from any source."""
        # TODO: integrate with load_input to remove duplicated code.
        input_type = type
        input_name = name
        path = path.replace('s3://', 's3a://') if self.jargs.mode == 'dev_local' else path
        logger.info("Dataset '{}' to be loaded from files '{}'.".format(input_name, path))
        path = Path_Handler(path, self.jargs.base_path).expand_later(self.jargs.storage)

        if input_type == 'txt':
            rdd = self.sc.textFile(path)
            logger.info("Dataset '{}' loaded from files '{}'.".format(input_name, path))
            return rdd

        # Tabular types
        if input_type == 'csv':
            sdf = sc_sql.read.csv(path, header=True)  # TODO: add way to add .option("delimiter", ';'), useful for metric_budgeting.
            logger.info("Dataset '{}' loaded from files '{}'.".format(input_name, path))
        elif input_type == 'parquet':
            # TODO: check to add ...read.option("mergeSchema", "true").parquet...
            sdf = sc_sql.read.parquet(path)
            logger.info("Dataset '{}' loaded from files '{}'.".format(input_name, path))
        else:
            raise Exception("Unsupported dataset type '{}' for path '{}'. Supported types are: {}. ".format(input_type, path, self.SUPPORTED_TYPES))

        # New param "custom_schema" to work for both db and file inputs (instead of just db). TODO: finish.
        # df_custom_schema = self.jargs.merged_args.get('df_custom_schema')
        # if df_custom_schema:
        #     for field, type in df_custom_schema.items():
        #         table_to_copy = table_to_copy.withColumn(field, table_to_copy[field].cast(type))

        logger.info("Dataset data types: {}".format(pformat([(fd.name, fd.dataType) for fd in sdf.schema.fields])))
        return sdf

    def load_mysql(self, input_name):
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, creds=self.jargs.connection_file)
        creds_section = self.jargs.inputs[input_name]['creds']
        db = creds[creds_section]
        extra_params = '' # can use '?zeroDateTimeBehavior=CONVERT_TO_NULL' to help solve "java.sql.SQLException: Zero date value prohibited" but leads to other error msg.
        url = 'jdbc:mysql://{host}:{port}/{service}{extra_params}'.format(host=db['host'], port=db['port'], service=db['service'], extra_params=extra_params)
        dbtable = self.jargs.inputs[input_name]['db_table']
        inc_field = self.jargs.inputs[input_name].get('inc_field')
        if not inc_field:
            logger.info('Pulling table "{}" from mysql'.format(dbtable))
            sdf = self.sc_sql.read \
                .format('jdbc') \
                .option('driver', "com.mysql.cj.jdbc.Driver") \
                .option("url", url) \
                .option("user", db['user']) \
                .option("password", db['password']) \
                .option("dbtable", dbtable)\
                .load()
        else:
            inc_field = self.jargs.inputs[input_name]['inc_field']
            period = self.period
            # query_str = "select * from {} where {} = '{}'".format(dbtable, inc_field, period)
            higher_limit = "AND {inc_field} < '{period_next}'".format(inc_field=inc_field, period_next=self.period_next) if self.period_next else ''
            query_str = "select * from {dbtable} where {inc_field} >= '{period}' {higher_limit}".format(dbtable=dbtable, inc_field=inc_field, period=self.period, higher_limit=higher_limit)
            logger.info('Pulling table from mysql with query_str "{}"'.format(query_str))
            # if self.jargs.merged_args.get('custom_schema', '')
            #         db_overridden_types_str = ', '.join([k + ' ' + v for k, v in db_overridden_types.items()])

            # TODO: check if it should use com.mysql.cj.jdbc.Driver instead as above
            sdf = self.sc_sql.read \
                .format('jdbc') \
                .option('driver', "com.mysql.jdbc.Driver") \
                .option('fetchsize', 10000) \
                .option('numPartitions', 3) \
                .option("url", url) \
                .option("user", db['user']) \
                .option("password", db['password']) \
                .option("customSchema", self.jargs.merged_args.get('jdbc_custom_schema', '')) \
                .option("query", query_str) \
                .load()
        return sdf

    def load_clickhouse(self, input_name):
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, creds=self.jargs.connection_file)
        creds_section = self.jargs.inputs[input_name]['creds']
        db = creds[creds_section]
        url = 'jdbc:postgresql://{host}/{service}'.format(host=db['host'], service=db['service'])
        dbtable = self.jargs.inputs[input_name]['db_table']
        inc_field = self.jargs.inputs[input_name].get('inc_field')
        if not inc_field:
            logger.info('Pulling table "{}" from Clickhouse'.format(dbtable))
            sdf = self.sc_sql.read \
                .format('jdbc') \
                .option('driver', "org.postgresql.Driver") \
                .option("url", url) \
                .option("user", db['user']) \
                .option("password", db['password']) \
                .option("dbtable", dbtable)\
                .load()
        else:
            inc_field = self.jargs.inputs[input_name]['inc_field']
            period = self.period
            query_str = "select * from {} where {} = '{}'".format(dbtable, inc_field, period)
            logger.info('Pulling table from Clickhouse with query_str "{}"'.format(query_str))
            sdf = self.sc_sql.read \
                .format('jdbc') \
                .option('driver', "org.postgresql.Driver") \
                .option('fetchsize', 10000) \
                .option('numPartitions', 3) \
                .option("url", url) \
                .option("user", db['user']) \
                .option("password", db['password']) \
                .option("query", query_str) \
                .load()
        return sdf

    def get_previous_output_max_timestamp(self, sc, sc_sql):
        path = self.jargs.output['path']  # implies output path is incremental (no "{now}" in string.)
        path += '*' if self.jargs.merged_args.get('incremental_type') == 'no_schema' else '' # '*' to go into output subfolders.
        try:
            df = self.load_data_from_files(name='output', path=path, type=self.jargs.output['type'], sc=sc, sc_sql=sc_sql)
        except Exception as e:  # TODO: don't catch all
            logger.info("Previous increment could not be loaded or doesn't exist. It will be ignored. Folder '{}' failed loading with error '{}'.".format(path, e))
            return None

        dt = self.get_max_timestamp(df)
        logger.info("Max timestamp of previous increment: '{}'".format(dt))
        return dt

    def get_max_timestamp(self, df):
        return df.agg({self.jargs.output['inc_field']: "max"}).collect()[0][0]

    def save_output(self, output, now_dt=None):
        self.path = self.save(output=output,
                  path=self.jargs.output['path'],
                  base_path=self.jargs.base_path,
                  type=self.jargs.output['type'],
                  now_dt=now_dt,
                  is_incremental=self.jargs.is_incremental,
                  incremental_type=self.jargs.merged_args.get('incremental_type', 'no_schema'),
                  partitionby=self.jargs.output.get('inc_field') or self.jargs.merged_args.get('partitionby'),
                  file_tag=self.jargs.merged_args.get('file_tag'))  # TODO: make param standard in cmd_args ?

    def save(self, output, path, base_path, type, now_dt=None, is_incremental=None, incremental_type=None, partitionby=None, file_tag=None):
        """Used to save output to disk. Can be used too inside jobs to output 2nd output for testing."""
        path = Path_Handler(path, base_path).expand_now(now_dt)

        if type == 'None':
            logger.info('Did not write output to disk')
            return None

        if is_incremental and incremental_type == 'no_schema':
            current_time = now_dt.strftime('%Y%m%d_%H%M%S_utc')  # no use of now_dt to make it updated for each inc.
            file_tag = ('_' + file_tag) if file_tag else ""  # TODO: make that param standard in cmd_args ?
            path += 'inc_{}{}/'.format(current_time, file_tag)

        # TODO: rename 'partitioned' to 'spark_partitions' and 'no_schema' to 'yaetos_partitions'
        write_mode = 'append' if incremental_type == 'partitioned' or partitionby else 'error'
        partitionby = partitionby.split(',') if partitionby else []

        # TODO: deal with cases where "output" is df when expecting rdd, or at least raise issue in a cleaner way.
        if type == 'txt':
            output.saveAsTextFile(path)
        elif type == 'parquet':
            output.write.partitionBy(*partitionby).mode(write_mode).parquet(path)
        elif type == 'csv':
            output.write.partitionBy(*partitionby).mode(write_mode).option("header", "true").csv(path)
        else:
            raise Exception("Need to specify supported output type, either txt, parquet or csv.")

        logger.info('Wrote output to ' + path)
        return path

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
            """%(self.app_name, self.jargs.job_name, elapsed)
        FS_Ops_Dispatcher().save_metadata(fname, content, self.jargs.storage)

    def query(self, query_str):
        logger.info('Query string:\n' + query_str)
        df =  self.sc_sql.sql(query_str)
        df.cache()
        return df

    def copy_to_redshift_using_pandas(self, output, types):
        # import put here below to avoid loading heavy libraries when not needed (optional feature).
        from yaetos.redshift_pandas import create_table
        from yaetos.db_utils import cast_col
        df = output.toPandas()
        df = cast_col(df, types)
        connection_profile = self.jargs.copy_to_redshift['creds']
        schema, name_tb = self.jargs.copy_to_redshift['table'].split('.')
        schema = schema.format(schema=self.jargs.schema) if '{schema}' in schema else schema
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, creds=self.jargs.connection_file)
        create_table(df, connection_profile, name_tb, schema, types, creds, self.jargs.is_incremental)
        del(df)

    def copy_to_redshift_using_spark(self, sdf):
        # import put here below to avoid loading heavy libraries when not needed (optional feature).
        from yaetos.redshift_spark import create_table
        connection_profile = self.jargs.copy_to_redshift['creds']
        schema, name_tb= self.jargs.copy_to_redshift['table'].split('.')
        schema = schema.format(schema=self.jargs.schema) if '{schema}' in schema else schema
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, creds=self.jargs.connection_file)
        create_table(sdf, connection_profile, name_tb, schema, creds, self.jargs.is_incremental, self.jargs.redshift_s3_tmp_dir, self.jargs.merged_args.get('spark_version', '2.4'))

    def copy_to_clickhouse(self, sdf):
        # import put here below to avoid loading heavy libraries when not needed (optional feature).
        from yaetos.clickhouse import create_table
        connection_profile = self.jargs.copy_to_clickhouse['creds']
        schema, name_tb= self.jargs.copy_to_clickhouse['table'].split('.')
        schema = schema.format(schema=self.jargs.schema) if '{schema}' in schema else schema
        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, creds=self.jargs.connection_file)
        create_table(sdf, connection_profile, name_tb, schema, creds, self.jargs.is_incremental)

    def push_to_kafka(self, output, types):
        """ Needs to be overriden by each specific job."""
        raise NotImplementedError

    def send_msg(self, msg, recipients=None):
        """ Sending message to recipients (list of email addresse) or, if not specified, to yml 'owners'.
        Pulling email sender account info from connection_file."""
        if not recipients:
            recipients = self.jargs.merged_args.get('owners')
        if not recipients:
            logger.error("Email can't be sent since no recipient set in {}, .\nMessage : \n{}".format(self.jargs.job_param_file, msg))
            return None

        creds = Cred_Ops_Dispatcher().retrieve_secrets(self.jargs.storage, creds=self.jargs.connection_file)
        creds_section = self.jargs.email_cred_section

        sender_email = creds.get(creds_section, 'sender_email')
        password = creds.get(creds_section, 'password')
        smtp_server = creds.get(creds_section, 'smtp_server')
        port = creds.get(creds_section, 'port')

        for recipient in recipients:
            send_email(msg, recipient, sender_email, password, smtp_server, port)
            logger.info('Email sent to {}'.format(recipient))

    def send_job_failure_email(self, error_msg):
        message = """Subject: [Data Pipeline Failure] {name}\n\nA Data pipeline named '{name}' failed.\nError message:\n{error}\n\nPlease check logs in AWS.""".format(name=self.jargs.job_name, error=error_msg)
        self.send_msg(message)

    @staticmethod
    def check_pk(df, pks):
        count = df.count()
        count_pk = df.select(pks).dropDuplicates().count()
        if count != count_pk:
            logger.error("PKs not unique. count={}, count_pk={}".format(count, count_pk))
            return False
        else:
            logger.info("Confirmed fields given are PKs (i.e. unique). count=count_pk={}".format(count))
            return True

    def identify_non_unique_pks(self, df, pks):
        windowSpec  = Window.partitionBy([F.col(item) for item in pks])
        df = df.withColumn('_count_pk', F.count('*').over(windowSpec)) \
            .where(F.col('_count_pk') >= 2)
        # Debug: df.repartition(1).write.mode('overwrite').option("header", "true").csv('data/sandbox/non_unique_test/')
        return df

class Period_Builder():
    @staticmethod
    def get_last_day(as_of_date=datetime.utcnow()):
        last_day_dt = as_of_date + relativedelta(days=-1)
        last_day = last_day_dt.strftime("%Y-%m-%d")
        return last_day

    @staticmethod
    def get_first_to_last_day(first_day, as_of_date=datetime.utcnow()):
        now = as_of_date
        start = datetime.strptime(first_day, "%Y-%m-%d")
        delta = now - start
        number_days = delta.days

        periods = []
        iter_days = start
        for item in range(number_days):
            periods.append(iter_days.strftime("%Y-%m-%d"))
            iter_days = iter_days + relativedelta(days=+1)
        return periods

    def get_last_output_to_last_day(self, last_run_period, first_day_input, as_of_date=datetime.utcnow()):
        periods = self.get_first_to_last_day(first_day_input, as_of_date)
        if last_run_period:
            periods = [item for item in periods if item > last_run_period]
        # periods = [item for item in periods if item < '2021-01-02']  # TODO: make end period parametrizable from args.
        return periods


class Schema_Builder():
    TYPES_FOLDER = 'schemas/'
    def generate_schemas(self, loaded_datasets, output):
        yml = {'inputs':{}}
        for key, value in loaded_datasets.items():
            yml['inputs'][key] = {fd.name: fd.dataType.__str__() for fd in value.schema.fields}
        yml['output'] = {fd.name: fd.dataType.__str__() for fd in output.schema.fields}
        self.yml = yml

    def save_yaml(self, job_name):
        job_name = job_name.replace('.py', '')
        fname = self.TYPES_FOLDER + job_name+'.yaml'
        os.makedirs(os.path.dirname(fname), exist_ok=True)
        with open(fname, 'w') as file:
            ignored = yaml.dump(self.yml, file)


class Job_Yml_Parser():
    """Functions to load and parse yml, and functions to get job_name, which is the key to the yml info."""

    def __init__(self, job_name, job_param_file, mode, skip_job=False):
        self.yml_args = self.set_job_yml(job_name, job_param_file, mode, skip_job)
        self.yml_args['job_name'] = job_name
        self.yml_args['py_job'] = self.yml_args.get('py_job') or self.set_py_job_from_name(job_name)
        self.yml_args['sql_file'] = self.set_sql_file_from_name(job_name, mode)

    @staticmethod
    def set_job_name_from_file(job_file):
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

    @staticmethod
    def set_py_job_from_name(job_name):
        py_job='jobs/{}'.format(job_name)
        logger.info("py_job: '{}', from job_name: '{}'".format(py_job, job_name))
        return py_job

    @staticmethod
    def set_sql_file_from_name(job_name, mode):
        if not job_name.endswith('.sql'):
            return None

        if mode in ('dev_EMR', 'prod_EMR'):
            sql_file=CLUSTER_APP_FOLDER+'jobs/{}'.format(job_name)
        elif mode == 'dev_local':
            sql_file='jobs/{}'.format(job_name)
        else:
            raise Exception("Mode not supported in set_sql_file_from_name(): {}".format(mode))

        logger.info("sql_file: '{}', from job_name: '{}'".format(sql_file, job_name))
        return sql_file

    def set_job_yml(self, job_name, job_param_file, yml_mode, skip_job):
        if job_param_file is None:
            return {}
        yml = self.load_meta(job_param_file)

        if job_name not in yml['jobs'] and not skip_job:
            raise KeyError("Your job '{}' can't be found in jobs_metadata file '{}'. Add it there or make sure the name matches".format(job_name, job_param_file))
        elif job_name not in yml['jobs'] and skip_job:
            job_yml = {}
        else:
            job_yml = yml['jobs'][job_name]

        if yml_mode not in yml['common_params']['mode_specific_params']:
            raise KeyError("Your yml mode '{}' can't be found in jobs_metadata file '{}'. Add it there or make sure the name matches".format(yml_mode, job_param_file))

        mode_spec_yml = yml['common_params']['mode_specific_params'][yml_mode]
        out = yml['common_params']['all_mode_params']
        out.update(mode_spec_yml)
        out.update(job_yml)
        return out

    @staticmethod
    def load_meta(fname):
        with open(fname, 'r') as stream:
            yml = yaml.load(stream)
        return yml


class Job_Args_Parser():

    DEPLOY_ARGS_LIST = ['aws_config_file', 'aws_setup', 'leave_on', 'push_secrets', 'frequency', 'start_date',
                        'email', 'mode', 'deploy', 'terminate_after', 'spark_version']

    def __init__(self, defaults_args, yml_args, job_args, cmd_args, job_name=None, loaded_inputs={}):
        """Mix all params, add more and tweak them when needed (like depending on storage type, execution mode...).
        If yml_args not provided, it will go and get it.
        Sets of params:
            - defaults_args: defaults command line args, as defined in define_commandline_args()
            - yml_args: args for specific job from yml. If = None, it will rebuild it using job_name param.
            - job_args: args passed to "Commandliner(Job, **args)" in each job file
            - cmd_args: args passed in commandline, like "python some_job.py --some_args=xxx", predefined in define_commandline_args() or not
            - job_name: to use only when yml_args is set to None, to specify what section of the yml to pick.
        """
        if yml_args is None:
            # Getting merged args, without yml (order matters)
            args = defaults_args.copy()
            args.update(job_args)
            args.update(cmd_args)
            args.update({'job_name':job_name} if job_name else {})
            args['mode'] = 'dev_EMR' if args['mode'] == 'dev_local' and args['deploy'] in ('EMR', 'EMR_Scheduled') else args['mode']
            assert 'job_name' in args.keys()
            yml_args = Job_Yml_Parser(args['job_name'], args['job_param_file'], args['mode'], args.get('skip_job', False)).yml_args

        # Get merged args, with yml (order matters)
        # TODO: need to add business of flatten/unflatten so they can be merged cleanely.
        args = defaults_args.copy()
        args.update(yml_args)
        args.update(job_args)
        args.update(cmd_args)
        args['mode'] = 'dev_EMR' if args['mode'] == 'dev_local' and args['deploy'] in ('EMR', 'EMR_Scheduled') else args['mode']
        args = self.update_args(args, loaded_inputs)

        [setattr(self, key, value) for key, value in args.items()]  # attach vars to self.*
        # Other access to vars
        self.merged_args = args
        self.defaults_args = defaults_args
        self.yml_args = yml_args
        self.job_args = job_args
        self.cmd_args = cmd_args
        logger.info("Job args: \n{}".format(pformat(args)))

    def get_deploy_args(self):
        return {key: value for key, value in self.merged_args.items() if key in self.DEPLOY_ARGS_LIST}

    def get_app_args(self):
        return {key: value for key, value in self.merged_args.items() if key not in self.DEPLOY_ARGS_LIST or key=='mode'}

    def update_args(self, args, loaded_inputs):
        """ Updating params or adding new ones, according to execution environment (local, prod...)"""
        args['inputs'] = self.set_inputs(args, loaded_inputs)
        # args['output'] = self.set_output(cmd_args, yml_args)  # TODO: fix later
        args['is_incremental'] = self.set_is_incremental(args.get('inputs', {}), args.get('output', {}))
        args['output']['type'] = args.pop('output.type', None) or args['output']['type']
        return args

    # TODO: modify later since not used now
    def set_inputs(self, args, loaded_inputs):
        # inputs_in_args = any([item.startswith('input_') for item in cmd_args.keys()])
        # if inputs_in_args:
        #     # code below limited, will break in non-friendly way if not all input params are provided, doesn't support other types of inputs like db ones. TODO: make it better.
        #     input_paths = {key.replace('input_path_', ''): {'path': val} for key, val in cmd_args.items() if key.startswith('input_path_')}
        #     input_types = {key.replace('input_type_', ''): {'type': val} for key, val in cmd_args.items() if key.startswith('input_type_')}
        #     inputs = {key: {'path': val['path'], 'type':input_types[key]['type']} for key, val in input_paths.items()}
        #     return inputs
        if loaded_inputs:
            return {key: {'path': val, 'type': 'df'} for key, val in loaded_inputs.items()}
        else:
            return args.get('inputs', {})

    # TODO: modify later since not used now
    # def set_output(self, cmd_args, yml_args):
    #     output_in_args = any([item == 'output_path' for item in cmd_args.keys()])
    #     if output_in_args:
    #         # code below limited, will break in non-friendly way if not all output params are provided, doesn't support other types of outputs like db ones. TODO: make it better.
    #         output = {'path':cmd_args['output_path'], 'type':cmd_args['output_type']}
    #         return output
    #     elif cmd_args.get('job_param_file'):  # should be before loaded_inputs to use yaml if available. Later function load_inputs uses both self.jargs.inputs and loaded_inputs, so not incompatible.
    #         return yml_args.get('output', {})
    #     elif cmd_args.get('mode_no_io'):
    #         output = {}
    #         logger.info("No output given")
    #     else:
    #         raise Exception("No output given")
    #     return output

    def set_is_incremental(self, inputs, output):
        return any(['inc_field' in inputs[item] for item in inputs.keys()]) or 'inc_field' in output


class FS_Ops_Dispatcher():
    # TODO: remove 'storage' var not used anymore accross all functions below, since now infered from path

    @staticmethod
    def is_s3_path(path):
        return path.startswith('s3://') or path.startswith('s3a://')

    # --- save_metadata set of functions ----
    def save_metadata(self, fname, content, storage):
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
    def save_file(self, fname, content, storage):
        self.save_file_cluster(fname, content) if self.is_s3_path(fname) else self.save_file_local(fname, content)

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
        s3c = boto3.Session(profile_name='default').client('s3')

        local_path = CLUSTER_APP_FOLDER+'tmp/local_'+fname_parts[-1]
        self.save_file_local(local_path, content)
        fh = open(local_path, 'rb')
        s3c.put_object(Bucket=bucket_name, Key=bucket_fname, Body=fh)
        logger.info("Pushed local file to S3, from '{}' to '{}' ".format(local_path, fname))

    # --- load_file set of functions ----
    def load_file(self, fname, storage):
        return self.load_file_cluster(fname) if self.is_s3_path(fname) else self.load_file_local(fname)

    @staticmethod
    def load_file_local(fname):
        return joblib.load(fname)

    @staticmethod
    def load_file_cluster(fname):
        fname_parts = fname.split('s3://')[1].split('/')
        bucket_name = fname_parts[0]
        bucket_fname = '/'.join(fname_parts[1:])
        local_path = CLUSTER_APP_FOLDER+'tmp/s3_'+fname_parts[-1]
        s3c = boto3.Session(profile_name='default').client('s3')
        s3c.download_file(bucket_name, bucket_fname, local_path)
        logger.info("Copied file from S3 '{}' to local '{}'".format(fname, local_path))
        model = joblib.load(local_path)
        return model

    # --- listdir set of functions ----
    def listdir(self, path, storage):
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
    def dir_exist(self, path, storage):
        return self.dir_exist_cluster(path) if self.is_s3_path(path) else self.dir_exist_local(path)

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
        client = boto3.Session(profile_name='default').client('secretsmanager')

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
    def __init__(self, path, base_path=None):
        if base_path:
            path = path.format(base_path=base_path, latest='{latest}', now='{now}')
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
    def __init__(self, Job, **job_args):
        parser, defaults_args = self.define_commandline_args()
        cmd_args = self.set_commandline_args(parser)

        # Building "job", which will include all job args.
        if Job is None:  # when job run from "python launcher.py --job_name=some_name_from_job_metadata_file"
            jargs = Job_Args_Parser(defaults_args=defaults_args, yml_args=None, job_args=job_args, cmd_args=cmd_args, loaded_inputs={})
            Job = get_job_class(jargs.py_job)
            job = Job(jargs=jargs)
        else:  # when job run from "python some_job.py"
            job = Job(pre_jargs={'defaults_args':defaults_args, 'job_args': job_args, 'cmd_args':cmd_args})  # can provide jargs directly here since job_file (and so job_name) needs to be extracted from job first. So, letting job build jargs.

        # Executing or deploying
        if job.jargs.deploy in ('none'):  # when executing job code
            self.launch_run_mode(job)
        elif job.jargs.deploy in ('EMR', 'EMR_Scheduled', 'code'):  # when deploying to AWS for execution there
            self.launch_deploy_mode(job.jargs.get_deploy_args(), job.jargs.get_app_args())

    @staticmethod
    def set_commandline_args(parser):
        """Command line arguments take precedence over function ones."""
        cmd_args, cmd_unknown_args = parser.parse_known_args()
        cmd_args = {key: value for (key, value) in cmd_args.__dict__.items() if value is not None}
        cmd_unknown_args = dict([item[2:].split('=') for item in cmd_unknown_args])  # imposes for unknown args to be defined with '=' and to start with '--'
        cmd_args.update(cmd_unknown_args)
        return cmd_args

    @staticmethod
    def define_commandline_args():
        # Defined here separatly for overridability.
        parser = argparse.ArgumentParser()
        parser.add_argument("-d", "--deploy", choices=set(['none', 'EMR', 'EMR_Scheduled', 'EMR_DataPipeTest', 'code']), help="Choose where to run the job.")
        parser.add_argument("-m", "--mode", choices=set(['dev_local', 'dev_EMR', 'prod_EMR']), help="Choose which set of params to use from jobs_metadata.yml file.")
        parser.add_argument("-j", "--job_param_file", help="Identify file to use. It can be set to 'False' to not load any file and provide all parameters through job or command line arguments.")
        parser.add_argument("-n", "--job_name", help="Identify registry job to use.")
        parser.add_argument("-q", "--sql_file", help="Path to an sql file to execute.")
        parser.add_argument("--connection_file", help="Identify file to use. Default to repo one.")
        parser.add_argument("--jobs_folder", help="Identify the folder where job code is. Necessary if job code is outside the repo, i.e. if this is used as an external library. By default, uses the repo 'jobs/' folder.")
        parser.add_argument("-s", "--storage", choices=set(['local', 's3']), help="Choose 'local' (default) or 's3'.")
        parser.add_argument("-x", "--dependencies", action='store_true', help="Run the job dependencies and then the job itself")
        parser.add_argument("-c", "--rerun_criteria", choices=set(['last_date', 'output_empty', 'both']), help="Choose criteria to rerun the next increment or not. 'last_date' usefull if we know data goes to a certain date. 'output_empty' not to be used if increment may be empty but later ones not. Only relevant for incremental job.")
        parser.add_argument("-b", "--boxed_dependencies", action='store_true', help="Run dependant jobs in a sandboxed way, i.e. without passing output to next step. Only useful if ran with dependencies (-x).")
        parser.add_argument("-l", "--load_connectors", choices=set(['all', 'none']), help="Load java packages to enable spark connectors (s3, redshift, mysql). Set to 'none' to have faster spark start time and smaller log when connectors are not necessary. Only useful when mode=dev_local.")
        parser.add_argument("-t", "--output.type", choices=set(['csv', 'parquet']), help="Override output type. Useful for development. Can be ignored otherwise.")
        # Deploy specific
        parser.add_argument("--aws_config_file", help="Identify file to use. Default to repo one.")
        parser.add_argument("-a", "--aws_setup", help="Choose aws setup from conf/aws_config.cfg, typically 'prod' or 'dev'. Only relevant if choosing to deploy to a cluster.")
        parser.add_argument("-o", "--leave_on", action='store_true', help="Use arg to not terminate cluster after running the job. Mostly for testing. Only relevant when creating a new cluster when deploy=EMR.")
        parser.add_argument("-p", "--push_secrets", action='store_true', help="Pushing secrets to cluster. Only relevant if choosing to deploy to a cluster.")
        # --inputs and --output args can be set from job or commandline too, just not set here.
        defaults = {
                    'deploy': 'none',
                    'mode': 'dev_local',
                    'job_param_file': JOBS_METADATA_FILE,
                    'job_name': None,
                    'sql_file': None,
                    'connection_file': CONNECTION_FILE,
                    'jobs_folder': JOB_FOLDER,
                    'storage': 'local',
                    # 'dependencies': False, # only set from commandline
                    'rerun_criteria': 'last_date',
                    # 'boxed_dependencies': False,  # only set from commandline
                    'load_connectors': 'all',
                    # 'output.type': 'csv',  # skipped on purpose to avoid setting it if not set in cmd line.
                    #-- Deploy specific below --
                    'aws_config_file': AWS_CONFIG_FILE,
                    'aws_setup': 'dev',
                    # 'leave_on': False, # only set from commandline
                    # 'push_secrets': False, # only set from commandline
                    #-- Not added in command line args:
                    'enable_redshift_push': True,
                    'base_path': '',
                    'save_schemas': False,
                    'manage_git_info': False,
                    'add_created_at': 'true',  # set as string to be overrideable in cmdline.
                    'no_fw_cache': False,
                    }
        return parser, defaults

    def launch_run_mode(self, job):
        app_name = job.jargs.job_name
        sc, sc_sql = self.create_contexts(app_name, job.jargs) # TODO: set spark_version default upstream, remove it from here and from deploy.py.
        if not job.jargs.dependencies:
            job.etl(sc, sc_sql)
        else:
            Flow(job.jargs, app_name).run_pipeline(sc, sc_sql)

    def launch_deploy_mode(self, deploy_args, app_args):
        # Load deploy lib here instead of at module level to remove dependency on it when running code locally
        from yaetos.deploy import DeployPySparkScriptOnAws
        DeployPySparkScriptOnAws(deploy_args, app_args).run()

    def create_contexts(self, app_name, jargs):
        # Load spark here instead of at module level to remove dependency on spark when only deploying code to aws.
        from pyspark.sql import SQLContext
        from pyspark.sql import SparkSession
        from pyspark import SparkConf

        conf = SparkConf()
        # TODO: move spark-submit params here since it is more generic than in spark submit, params like "spark.driver.memoryOverhead" cause pb in spark submit.

        if jargs.merged_args.get('driver-memoryOverhead'):  # For extra overhead for python in driver (typically pandas)
            conf = conf.set("spark.driver.memoryOverhead", jargs.merged_args['driver-memoryOverhead'])

        if jargs.mode == 'dev_local' and jargs.load_connectors == 'all':
            # Env vars for S3 access
            credentials = boto3.Session(profile_name='default').get_credentials()
            os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
            # JARs
            package = PACKAGES_LOCAL if jargs.merged_args.get('spark_version', '2.4') == '2.4' else PACKAGES_LOCAL_ALT
            package_str = ','.join(package)
            conf = conf \
                .set("spark.jars.packages", package_str) \
                .set("spark.jars", JARS)
            # Setup above not needed when running from EMR where setup done in spark-submit.

        if jargs.merged_args.get('emr_core_instances') == 0:
            conf = conf \
                .set("spark.hadoop.fs.s3a.buffer.dir", '/tmp') \
                .set("spark.hadoop.fs.s3a.fast.upload.active.blocks", '1')

        spark = SparkSession.builder \
            .appName(app_name) \
            .config(conf=conf) \
            .getOrCreate()

        sc = spark.sparkContext
        sc_sql = SQLContext(sc)
        logger.info('Spark Config: {}'.format(sc.getConf().getAll()))
        return sc, sc_sql


class Flow():
    def __init__(self, launch_jargs, app_name):
        self.app_name = app_name
        df = self.create_connections_jobs(launch_jargs.storage, launch_jargs.merged_args)
        logger.debug('Flow app_name : {}, connection_table: {}'.format(app_name, df))
        graph = self.create_global_graph(df)  # top to bottom
        tree = self.create_local_tree(graph, nx.DiGraph(), app_name) # bottom to top
        self.leafs = self.get_leafs(tree, leafs=[]) # bottom to top
        logger.info('Sequence of jobs to be run: {}'.format(self.leafs))
        logger.info('-'*80)
        logger.info('-')
        launch_jargs.cmd_args.pop('job_name', None)  # removing since it should be pulled from yml and not be overriden by cmd_args.
        launch_jargs.job_args.pop('job_name', None)  # same
        self.launch_jargs = launch_jargs

    def run_pipeline(self, sc, sc_sql):
        """Load all job classes and run them"""
        df = {}
        for job_name in self.leafs:
            logger.info('About to run job_name: {}'.format(job_name))
            # Get yml
            yml_args = Job_Yml_Parser(job_name, self.launch_jargs.job_param_file, self.launch_jargs.mode).yml_args
            # Get loaded_inputs
            loaded_inputs = {}
            if not self.launch_jargs.boxed_dependencies:
                if yml_args.get('inputs', 'no input') == 'no input':
                    raise Exception("Pb with loading job_yml or finding 'inputs' parameter in it. You can work around it by using 'boxed_dependencies' argument.")
                for in_name, in_properties in yml_args['inputs'].items():
                    if in_properties.get('from'):
                        loaded_inputs[in_name] = df[in_properties['from']]

            # Get jargs
            jargs = Job_Args_Parser(self.launch_jargs.defaults_args, yml_args, self.launch_jargs.job_args, self.launch_jargs.cmd_args, loaded_inputs=loaded_inputs)

            Job = get_job_class(yml_args['py_job'])
            job = Job(jargs=jargs, loaded_inputs=loaded_inputs)
            df[job_name] = job.etl(sc, sc_sql) # at this point df[job_name] is unpersisted. TODO: keep it persisted.

            if self.launch_jargs.boxed_dependencies:
                df[job_name].unpersist()
                del df[job_name]
                gc.collect()
            logger.info('-'*80)
            logger.info('-')

    @staticmethod
    def create_connections_jobs(storage, args):
        yml = Job_Yml_Parser.load_meta(args['job_param_file'])

        connections = []
        for job_name, job_meta in yml['jobs'].items():
            dependencies = job_meta.get('dependencies') or []
            for dependency in dependencies:
                row = {'source_job': dependency, 'destination_job': job_name}
                connections.append(row)

        return pd.DataFrame(connections)

    @staticmethod
    def create_global_graph(df):
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

def send_email(message, receiver_email, sender_email, password, smtp_server, port):
    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.starttls(context=context)
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)
