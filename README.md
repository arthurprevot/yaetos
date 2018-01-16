# pyspark_aws_etl

This is a framework to write ETLs on top of [spark](http://spark.apache.org/) (the python binding, pyspark) and deploy them to Amazon Web Services (AWS) to run on datasets from S3. It is meant to scale to large datasets. The emphasis was on simplicity. An ETL job can consists of an SQL file only. All job input and output definitions are all in a human readable yaml file. For simple jobs, there is no need to do any programming (apart from SQL). For more complex job, you can use the full power of Spark.

Some features:
 * Ability to develop locally for faster iteration time, and run on cluster when ready
 * Support dependencies across jobs
 * Support incremental loading and processing
 * When launch is cluster mode, the job can create a new cluster and run on it, or it can run on an existing cluster.
 * ETL code git control-able and unit-testable (although unit-testing not implemented currently)
 * Can integrate with any python library or spark-ml to build machine learning applications

## To try it

Run the installation instructions (see lower) and run the [sql example](jobs/examples/ex1_full_sql_job.sql) provided with:

    python core/sql_job.py  --sql_file=jobs/examples/ex1_full_sql_job.sql

It will run locally, taking the inputs from [here](conf/jobs_metadata_local.yml#L1-L4), transform them using this [sql job](jobs/examples/ex1_full_sql_job.sql) using sparkSQL engine, and dump the output [here](conf/jobs_metadata_local.yml#L5). To run that same sql example on an AWS cluster, add a `-d` argument at the command line above. In that case, inputs and outputs will be taken from S3 at [these locations](conf/jobs_metadata.yml#L1-L5). If you don't have a cluster available, it will create one and terminate it after the job is run. You will see the status on the job process in the "steps" tab of your AWS EMR web page.

To run an ETL that showcases manipulation of a spark dataframes, more flexible than the sql example above, run [this frameworked pyspark example](jobs/examples/ex1_frameworked_job.py):

    python jobs/examples/ex1_frameworked_job.py

To try an example with job dependencies, [try this](jobs/examples/ex4_dependency4_job.py):

    python jobs/examples/ex4_dependency4_job.py -x

It will run all 3 dependencies defined in [the job registry](conf/jobs_metadata_local.yml#L34-L55). There are other examples [here](jobs/examples/).

## Dev Flow

To write a new ETL, create a new file in [this folder](jobs/) or any subfolders, either a `.sql` file or a `.py` file, following the examples from that same folder, and register that job, its inputs and output path locations in [this file](conf/jobs_metadata.yml) to run the AWS cluster or in [this file](conf/jobs_metadata_local.yml) to run locally. To run the jobs, execute the command lines following the same patterns as above:

    python core/sql_job.py  --sql_file=jobs/examples/same_sql_file.sql
    # or
    python jobs/examples/ex1_frameworked_job.py

And add the `-d` to deploy and run on AWS cluster.

You can specify dependencies in the job registry, for local jobs (`conf/jobs_metadata_local.yml`) or on AWS cluster (`conf/jobs_metadata.yml`).

## Installation instructions

Copy the AWS config file `conf/config.cfg.example`, save it as `conf/config.cfg`, and fill in your AWS setup. You should also have your `~/.aws` folder setup (by `aws` command line) with your corresponding AWS account information.

If you have spark (tested with v2.1.0) installed, then you can just use it. If not, you can run the job from a docker container, which has spark and all python libraries already setup. A Dockerfile is included to create this container.

    cd path_to_repo_folder
    docker build -t spark_container .
    docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -v pyspark_aws_etl:/mnt/pyspark_aws_etl -v ~/.aws:/root/.aws -h spark spark_container

It will bring inside the container bash terminal, from where you can run the jobs. You need to run `./setup.sh`, from your host machine or from within the docker container depending on how you prefer to run spark.

If you want to run the example jobs, then you need to run `./setup_examples.sh`, again from your host machine or from within the docker container. It will download some small input dataset to your computer and push it to amazon S3 storage. Note that it involves creating a bucket on your S3 account manually and setting its name at the top of `./setup_examples.sh`.

## Potential improvements

 * addition of unit-testing
 * integration with scheduling tools (oozie)
 * automatic pulling/pushing data from s3 to local (sampled) for local development
 * easier reconciliation
 * more testing with large datasets and complex dependencies
 * ...

Lots of room for improvement. Contributions welcome.
