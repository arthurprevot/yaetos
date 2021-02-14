# Before running this script:
# replace ~/code/pyspark_aws_etl below by location of your pyspark_aws_etl repo.
# add : "-v ~/path/to/your/repo/pipelines:/mnt/external_pipelines \" if putting the pipelines in an external repo.
# add : "-v ~/.aws:/.aws \" to use the tool to run jobs in AWS (ad-hoc or scheduled). Requires awcli setup on host (with ~/.aws setup with profile "default").
# other options to be added if necessary:
# --cpu-shares \
# --cpus 6 \
# --memory 6g \


docker build -t pyspark_container .  # '.' matters
docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 \
    -v ~/code/pyspark_aws_etl:/mnt/pyspark_aws_etl \
    -h spark \
    pyspark_container
