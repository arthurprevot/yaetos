from pyspark import SparkContext
# from conf.scheduling import schedule_local as schedule  # TODO for testing

class etl():
    def run(self, sc, **kwargs):
        raise NotImplementedError

    def runner(self, sc):
        # import ipdb; ipdb.set_trace()

        new_args = {}
        for item in self.INPUTS.keys():
            new_args[item] = sc.textFile(self.INPUTS[item]['path'])

        output = self.run(sc, **new_args)
        output.saveAsTextFile(self.OUTPUT['path'])
        print 'Wrote output to ',self.OUTPUT['path']
        return output


def launch(classname, appName, app_file, aws):
    import sys
    process = 'run_local'
    if len(sys.argv) > 1:
        process = sys.argv[1]
    # data_location = sys.argv[2] # can be local or cluster.

    # sys.exit()
    if process == 'run_local':
        from pyspark import SparkContext
        # sc = SparkContext(appName="PythonWordCount")
        sc = SparkContext(appName=appName)
        # wordcount().runner(sc)
        classname().runner(sc)
    elif process == 'ship_cluster':
        from core.run import DeployPySparkScriptOnAws
        # DeployPySparkScriptOnAws(app_file="jobs/spark_example/wordcount_frameworked.py", setup='perso').run()
        DeployPySparkScriptOnAws(app_file=app_file, setup=aws).run()
