from pyspark import SparkContext
# from core.run import DeployPySparkScriptOnAws


class etl():
    def run(self, sc, **kwargs):
        raise NotImplementedError

    def runner(self, sc):
        # import ipdb; ipdb.set_trace()
        new_args = {item[0]:sc.textFile(item[1]) for item in schedule['wordcount']['inputs']}
        output = self.run(sc, **new_args)
        output.saveAsTextFile(schedule['wordcount']['output'])
        return output


schedule = {
    'wordcount':{
        'inputs': [
            ['lines',"s3://bucket-scratch/wordcount_test/input/sample_text.txt"],
            # ['lines',"sample_text.txt"],  # local
        ],
        'output': 's3://bucket-scratch/wordcount_test/output/v4/'
        # 'output': 'tmp/output_v5/'  # local
    }
}
