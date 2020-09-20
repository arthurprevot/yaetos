docker build -t pyspark_container .  # '.' matters
docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 \
    -v ~/code/pyspark_aws_etl:/mnt/pyspark_aws_etl \
    -v ~/.aws:/root/.aws \
    -h spark \
    pyspark_container
    # replace ~/code/pyspark_aws_etl by location of your pyspark_aws_etl repo.
    # add : "-v ~/code/repo_pipelines:/mnt/repo_pipelines \" if putting the pipelines in an external repo.
    # you can remove ~/.aws:/root/.aws if you don't intend sending jobs to AWS.
