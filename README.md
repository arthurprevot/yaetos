# pyspark_aws_etl

This is a framework to write ETLs on top of [spark](http://spark.apache.org/) (the python binding, pyspark) and deploy them to Amazon Web Services (AWS). It can run locally (using local datasets and running the process on your machine), or on AWS (using S3 datasets and running the process on an AWS EMR cluster). The emphasis was on simplicity while giving access to the full power of spark for processing large datasets. All job input and output definitions are in a human readable yaml file. In the simplest form, an ETL job consists of an SQL file (no need to know any programming for these). In more complex cases, an ETL consist of a python, giving access to Spark dataframes, RDDs and any python library.

Some features:
 * Running locally and on cluster
 * Support dependencies across jobs
 * Support incremental loading and processing
 * Create AWS cluster when needed or piggy back on an existing cluster.
 * ETL code git control-able and unit-testable (although unit-testing not implemented currently)
 * Can integrate with any python library or spark-ml to build machine learning applications or other.

## To try it

Run the installation instructions (see lower) and run [this sql example](jobs/examples/ex1_full_sql_job.sql) with:

    python core/sql_job.py  --sql_file=jobs/examples/ex1_full_sql_job.sql

It will run locally, taking the inputs from a job registry file (`jobs_metadata_local.yml`) at [ these lines](conf/jobs_metadata_local.yml#L1-L4), transform them based on this [ex1_full_sql_job.sql](jobs/examples/ex1_full_sql_job.sql) using sparkSQL engine, and dump the output [here](conf/jobs_metadata_local.yml#L5). To run that same sql example on an AWS cluster, add a `-d` argument at the command line above. In that case, inputs and outputs will be taken from S3 at [these locations](conf/jobs_metadata.yml#L1-L5) from the jobs_metadata file. If you don't have a cluster available, it will create one and terminate it after the job is run. You will see the status on the job process in the "steps" tab of your AWS EMR web page.

To run an ETL that showcases manipulation of a spark dataframes, more flexible than the sql example above, run this frameworked pyspark example [ex1_frameworked_job.py](jobs/examples/ex1_frameworked_job.py) with this:

    python jobs/examples/ex1_frameworked_job.py

To try an example with job dependencies, you can run [ex4_dependency4_job.py](jobs/examples/ex4_dependency4_job.py) with this:

    python jobs/examples/ex4_dependency4_job.py -x

It will run all 3 dependencies defined in [the jobs_metadata registry](conf/jobs_metadata_local.yml#L34-L55). There are other examples in [jobs/examples/](jobs/examples/).

## Dev Flow

To write a new ETL, create a new file in [ the `jobs/` folder](jobs/) or any subfolders, either a `.sql` file or a `.py` file, following the examples from that same folder, and register that job, its inputs and output path locations in [conf/jobs_metadata.yml](conf/jobs_metadata.yml) to run the AWS cluster or in [conf/jobs_metadata_local.yml](conf/jobs_metadata_local.yml) to run locally. To run the jobs, execute the command lines following the same patterns as above:

    python core/sql_job.py  --sql_file=jobs/examples/same_sql_file.sql
    # or
    python jobs/examples/ex1_frameworked_job.py

And add the `-d` to deploy and run on AWS cluster.

You can specify dependencies in the job registry, for local jobs or on AWS cluster.

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
