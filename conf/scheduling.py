
schedule = {
    'wordcount':{
        'inputs': {'lines':"s3://bucket-scratch/wordcount_test/input/sample_text.txt"},
        'output': 's3://bucket-scratch/wordcount_test/output/v5/',
    }
}

schedule_local = {
    'wordcount':{
        'inputs': {'lines':"sample_text.txt"},
        'output': 'tmp/output_v9/',
    }
}
