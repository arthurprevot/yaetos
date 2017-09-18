from pyspark import SparkContext
# from core.run import DeployPySparkScriptOnAws
from conf.scheduling import schedule_local as schedule  # TODO for testing

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
