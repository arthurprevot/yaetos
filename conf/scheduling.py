
schedule = {
    'wordcount':{
        'inputs': {'lines':"s3://bucket-scratch/wordcount_test/input/sample_text.txt"},
        'output': 's3://bucket-scratch/wordcount_test/output/v6/',
    },
    'worksi_chat':{
        'inputs': {'lines':"s3://bucket-scratch/wordcount_test/input/sample_text.txt"},
        'output': 's3://bucket-scratch/wordcount_test/output/v6/',
    }
}

schedule_local = {
    'wordcount':{
        'inputs': {'lines':"data/sample_text.txt"},
        'output': 'data/sample_text_output_v1/',
    },
    'worksi_chat':{
        'inputs': {'chats':{'path':"data/worksi_chat/dt=20170712/part-r-00000-c0204d2f-281b-4784-97da-df4ad9a113a1.gz.parquet", 'type':'parquet'}},
        'output': {'path':'data/worksi_chat_output_v4/', 'type':'csv'},
    }
}
