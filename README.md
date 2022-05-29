<p align="center">
	<img src="./docs/images/logo_full.jpeg" alt="Yaetos Project" width="300" height="auto"/>
</p>

<div align="center">

![Continuous Integration](https://github.com/arthurprevot/yaetos/actions/workflows/pythonapp.yml/badge.svg)
[![Users Documentation](https://img.shields.io/badge/-Users_Docs-blue?style=plastic&logo=readthedocs)](https://yaetos.readthedocs.io/en/latest/)
[![Medium](https://img.shields.io/badge/_-Medium-orange?style=plastic&logo=medium)](https://medium.com/@arthurprevot/yaetos-data-framework-description-ddc71caf6ce)

</div>

# Yaetos
Yaetos is a framework to write ETLs on top of [spark](http://spark.apache.org/) (the python binding, pyspark) and deploy them to Amazon Web Services (AWS). It can run locally (using local datasets and running the process on your machine), or on AWS (using S3 datasets and running the process on an AWS cluster). The emphasis is on simplicity while giving access to the full power of spark for processing large datasets. All job input and output definitions are in a human readable yaml file. It's name stands for "Yet Another ETL Tool on Spark".
 - In the simplest cases, an ETL job can consist of an SQL file only. No need to know any programming for these.
 - In more complex cases, an ETL job can consist of a python file, giving access to Spark dataframes, RDDs and any python library.

Some features:
 * Running locally and on cluster
 * Support dependencies across jobs
 * Support incremental loading and processing
 * Create AWS cluster when needed or piggy back on an existing cluster.
 * ETL code git control-able and unit-testable
 * Can integrate with any python library or spark-ml to build machine learning applications or other.

## To try it

Run the installation instructions (see lower) and run [this sql example](jobs/examples/ex1_full_sql_job.sql) with:

    python yaetos/sql_job.py  --sql_file=jobs/examples/ex1_full_sql_job.sql

It will run locally, taking the inputs from a job registry file (`jobs_metadata.yml`) at [these lines](conf/jobs_metadata.yml#L7-L11), transform them based on this [ex1_full_sql_job.sql](jobs/examples/ex1_full_sql_job.sql) using sparkSQL engine, and dump the output [here](conf/jobs_metadata.yml#12). To run the same sql example on an AWS cluster, add `--deploy=EMR` to the same command line above. In that case, inputs and outputs will be taken from S3 at [these locations](conf/jobs_metadata.yml#L1-L5) from the jobs_metadata file. If you don't have a cluster available, it will create one and terminate it after the job is finished. You can see the status on the job process in the "steps" tab of your AWS EMR web page.

To run an ETL that showcases manipulation of a spark dataframes, more flexible than the sql example above, run this frameworked pyspark example [ex1_frameworked_job.py](jobs/examples/ex1_frameworked_job.py) with this:

    python jobs/examples/ex1_frameworked_job.py

To try an example with job dependencies, run [ex4_dependency4_job.py](jobs/examples/ex4_dependency4_job.py) with this:

    python jobs/examples/ex4_dependency4_job.py --dependencies

It will run all 3 dependencies defined in [the jobs_metadata registry](conf/jobs_metadata.yml#L57-L87). There are other examples in [jobs/examples/](jobs/examples/).

## Development Flow

To write a new ETL, create a new file in [ the `jobs/` folder](jobs/) or any subfolders, either a `.sql` file or a `.py` file, following the examples from that same folder, and register that job, its inputs and output path locations in [conf/jobs_metadata.yml](conf/jobs_metadata.yml) to run the AWS cluster or in [conf/jobs_metadata.yml](conf/jobs_metadata.yml) to run locally. To run the jobs, execute the command lines following the same patterns as above:

    python yaetos/sql_job.py  --sql_file=jobs/examples/some_sql_file.sql
    # or
    python jobs/examples/ex1_frameworked_job.py

And add the `--deploy=EMR` to deploy and run on an AWS cluster.

You can specify dependencies in the job registry, for local jobs or on AWS cluster.

Jobs can be unit-tested using `py.test`. For a given job, create a corresponding job in `tests/jobs/` folder and add tests that relate to the specific business logic in this job. See [tests/jobs/ex1_frameworked_job_test.py](tests/jobs/ex1_frameworked_job_test.py)for an example.

## Unit-testing
... is done using `py.test`. Run them with:

    py.test tests/*  # for all tests
    py.test tests/jobs/examples/ex1_frameworked_job.py  # for tests for a specific file

## Installation instructions

To avoid installing dependencies on your machine manually, you can run the job from a docker container, with spark and python libraries already setup. The docker setup is included.

    pip install yaetos
    cd /path/to/an/empty/folder/that/will/contain/pipeline/code
    yaetos setup  # to create sub-folders and setup framework files.
    yaetos launch_env # to launch the docker container
    # From inside the docker container, try a test pipeline with
    python jobs/examples/ex1_frameworked_job.py --dependencies

The docker container is setup to share the current folder with the host, so ETL jobs can be written from your host machine, using any IDE, and run from the container directly.

To get jobs executed and/or scheduled in AWS, You need to:
 * fill AWS parameters in `conf/config.cfg`.
 * have `~/.aws/` folder setup to give access to AWS secret keys. If not, run `pip install  awscli`, and `aws configure`.

## Potential improvements

 * more unit-testing
 * integration with other scheduling tools (airflow...)
 * integration with other resource provisioning tools (kubernetes...)
 * automatic pulling/pushing data from s3 to local (sampled) for local development
 * easier dataset reconciliation
 * ...

Lots of room for improvement. Contributions welcome.
