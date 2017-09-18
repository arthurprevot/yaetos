
# schedule = {
#     'wordcount':{
#         'inputs': [
#             ['lines',"s3://bucket-scratch/wordcount_test/input/sample_text.txt"],
#             # ['lines',"sample_text.txt"],  # local
#         ],
#         'output': 's3://bucket-scratch/wordcount_test/output/v5/'
#         # 'output': 'tmp/output_v5/'  # local
#     }
# }

schedule_local = {
    'wordcount':{
        'inputs': [
            # ['lines',"s3://bucket-scratch/wordcount_test/input/sample_text.txt"],
            ['lines',"sample_text.txt"],  # local
        ],
        # 'output': 's3://bucket-scratch/wordcount_test/output/v5/'
        'output': 'tmp/output_v6/'  # local
    }
}
