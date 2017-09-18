from pyspark import SparkContext
# from core.run import DeployPySparkScriptOnAws
from conf.scheduling import schedule_local as schedule  # TODO for testing

class etl():
    def run(self, sc, **kwargs):
        raise NotImplementedError

    def runner(self, sc):
        # import ipdb; ipdb.set_trace()
        new_args = {item[0]:sc.textFile(item[1]) for item in schedule['wordcount']['inputs']}
        output = self.run(sc, **new_args)
        output.saveAsTextFile(schedule['wordcount']['output'])
        return output
