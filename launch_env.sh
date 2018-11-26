docker build -t pyspark_container .  # '.' matters
docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 \
    -v ~/code/pyspark_aws_etl:/mnt/pyspark_aws_etl \
    -v ~/.aws:/root/.aws \
    -h spark \
    pyspark_container
    # you can remove "-v ~/.aws:/root/.aws" if you don't intend sending jobs to AWS.
