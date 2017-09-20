import sys


class etl():
    def run(self, sc, **kwargs):
        raise NotImplementedError

    def runner(self, sc):
        new_args = {}
        for item in self.INPUTS.keys():
            new_args[item] = sc.textFile(self.INPUTS[item]['path'])

        output = self.run(sc, **new_args)
        output.saveAsTextFile(self.OUTPUT['path'])
        print 'Wrote output to ',self.OUTPUT['path']
        return output


def launch(classname, appName, app_file, aws):
    process = sys.argv[1] if len(sys.argv) > 1 else process = 'locally'

    if process == 'locally':
        from pyspark import SparkContext
        sc = SparkContext(appName=appName)
        classname().runner(sc)
    elif process == 'clusterly':
        from core.run import DeployPySparkScriptOnAws
        DeployPySparkScriptOnAws(app_file=app_file, setup=aws).run()
