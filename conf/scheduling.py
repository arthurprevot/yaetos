
schedule = {
    'wordcount':{
        'inputs': {'lines':{'path':"s3://bucket-scratch/wordcount_test/input/sample_text.txt", 'type':'txt'}},
        'output': {'path':'s3://bucket-scratch/wordcount_test/output/v6/', 'type':'csv'},
    },
    'worksi_chat':{
        'inputs': {'chats':{'path':"s3://asdf.txt", 'type':'parquet'}},
        'output': {'path':'s3://bucket-scratch/wordcount_test/output/v6/', 'type':'csv'},
    }
}

schedule_local = {
    'wordcount':{
        'inputs': {'lines':{'path':"data/sample_text.txt", 'type':'txt'}},
        'output': {'path':'data/sample_text_output_v1/', 'type':'txt'},
    },
    'worksi_chat':{
        'inputs': {'chats':{'path':"data/worksi_chat/dt=20170712/part-r-00000-c0204d2f-281b-4784-97da-df4ad9a113a1.gz.parquet", 'type':'parquet'}},
        'output': {'path':'data/worksi_chat_output_v4/', 'type':'csv'},
    }
}
