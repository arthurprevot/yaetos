"""
Helper functions. Setup to run locally and on cluster.
"""

import sys
import inspect
import yaml


JOBS_METADATA_FILE = 'conf/scheduling.yml'
CLUSTER_APP_FOLDER = '/home/hadoop/app/'


class etl(object):
    def run(self, sc, **kwargs):
        raise NotImplementedError

    def set_path(self, fname_schedule, app_name):
        """Can override this method to force paths regardless of job_meta file."""
        yml = self.load_schedule(fname_schedule)
        # import ipdb; ipdb.set_trace()
        self.INPUTS = yml[app_name]['inputs']
        self.OUTPUT = yml[app_name]['output']

    def runner(self, sc, sc_sql):
        self.sc = sc
        self.sc_sql = sc_sql
        # import ipdb; ipdb.set_trace()

        # TODO: isolate in fct
        run_args = {}
        for item in self.INPUTS.keys():
            if self.INPUTS[item]['type'] == 'txt':
                run_args[item] = sc.textFile(self.INPUTS[item]['path'])
            elif self.INPUTS[item]['type'] == 'csv':
                run_args[item] = sc_sql.read.csv(self.INPUTS[item]['path'], header=True)
                run_args[item].createOrReplaceTempView(item)
            elif self.INPUTS[item]['type'] == 'parquet':
                run_args[item] = sc_sql.read.parquet(self.INPUTS[item]['path'])
                run_args[item].createOrReplaceTempView(item)

        output = self.run(**run_args)

        # TODO: isolate in fct
        if self.OUTPUT['type'] == 'txt':
            output.saveAsTextFile(self.OUTPUT['path'])
        elif self.OUTPUT['type'] == 'parquet':
            output.write.parquet(self.OUTPUT['path'])
        elif self.OUTPUT['type'] == 'csv':
            output.write.csv(self.OUTPUT['path'])

        print 'Wrote output to ',self.OUTPUT['path']
        return output

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
    # import ipdb; ipdb.set_trace()
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
