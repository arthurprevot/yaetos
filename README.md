<p align="center">
	<img src="./docs/images/logo_full.jpeg" alt="Yaetos Project" width="300" height="auto"/>
</p>

<div align="center">

![Continuous Integration](https://github.com/arthurprevot/yaetos/actions/workflows/pythonapp.yml/badge.svg)
[![Users Documentation](https://img.shields.io/badge/-Users_Docs-blue?style=plastic&logo=readthedocs)](https://yaetos.readthedocs.io/en/latest/)
[![Medium](https://img.shields.io/badge/_-Medium-orange?style=plastic&logo=medium)](https://medium.com/@arthurprevot/yaetos-data-framework-description-ddc71caf6ce)

</div>

# Yaetos
Yaetos is a framework to write ETLs on top of [spark](http://spark.apache.org/) (the python binding, pyspark) or pandas and deploy them to Amazon Web Services (AWS). It can run locally (using local datasets and running the process on your machine), or on AWS (using S3 datasets and running the process on an AWS cluster). The emphasis is on simplicity while giving access to the full power of spark for processing large datasets. All job input and output definitions are in a human readable yaml file. It's name stands for "Yet Another ETL Tool on Spark"... although it can now also use pandas as an engine.
 - In the simplest cases, an ETL job can consist of an SQL file only. No need to know any programming for these.
 - In more complex cases, an ETL job can consist of a python file, giving access to Spark dataframes, RDDs and any python library.

Some features:
 * The ability to run jobs locally and on cluster without any changes.
 * The support of dependencies across jobs
 * The support of incremental loading and processing
 * The automatic creation of AWS clusters when needed.
 * The support for git and unit-tests
 * The integration with any python library or spark-ml to build machine learning or other pipelines.

## To try it

Run the installation instructions (see lower) and run [this sql example](jobs/examples/ex1_full_sql_job.sql) locally with:

		yaetos run_dockerized jobs/generic/launcher.py --job_name=examples/ex1_full_sql_job.sql

It will open the manifesto file (`jobs_metadata.yml`), find the job called `examples/ex1_full_sql_job.sql`, i.e. [these lines](conf/jobs_metadata.yml#L7-L16), get the job parameters from there (input paths, output path...), execute the transform defined in the job [ex1_full_sql_job.sql](jobs/examples/ex1_full_sql_job.sql) using sparkSQL engine, and dump the output [here](conf/jobs_metadata.yml#L12). To run the same sql example on an AWS cluster, add `--deploy=EMR` to the same command line above. In that case, inputs and outputs will be taken from S3, as defined by the `base_path` param in the manifesto [here](conf/jobs_metadata.yml#L214). If you don't have a cluster available, it will create one and terminate it after the job is finished. You can see the status on the job process in the "steps" tab of your AWS EMR web page.

To run an ETL that showcases manipulation of a spark dataframes, more flexible than the sql example above, run this frameworked pyspark example [ex1_frameworked_job.py](jobs/examples/ex1_frameworked_job.py) with this:

    python jobs/examples/ex1_frameworked_job.py

To try an example with job dependencies, run [ex4_dependency4_job.py](jobs/examples/ex4_dependency4_job.py) with this:

    python jobs/examples/ex4_dependency4_job.py --dependencies

It will run all 3 dependencies defined in [the jobs_metadata registry](conf/jobs_metadata.yml#L57-L87). There are other examples in [jobs/examples/](jobs/examples/).

As for the use of the yaetos command lines, note that executing:

	  yaetos run_dockerized some/job.py --some=args

is the same as executing:

		yaetos launch_docker_bash
		# and from inside the docker container:
		python some/job.py --some=args


To explore jobs in jupyter notebooks:

    yaetos launch_docker_jupyter  # from the host OS
		# open a browser on the host OS, and go to `http://localhost:8888/tree/notebooks`
		# navigate to sample notebooks, such as [inspect_ex4_dependencies4_job.ipynb](notebooks/inspect_ex4_dependencies4_job.ipynb)

## Development Flow

To write a new ETL, create a new file in [ the `jobs/` folder](jobs/) or any subfolders, either a `.sql` file or a `.py` file, following the examples from that same folder, and register that job, its inputs and output path locations in [conf/jobs_metadata.yml](conf/jobs_metadata.yml) to run the AWS cluster or in [conf/jobs_metadata.yml](conf/jobs_metadata.yml) to run locally. To run the jobs, execute the command lines following the same patterns as above:

    python jobs/generic/launcher.py --job_name=examples/some_sql_file.sql
    # or
    python jobs/examples/some_python_file.py

And add the `--deploy=EMR` to deploy and run on an AWS cluster.

You can specify dependencies in the job registry, for local jobs or on AWS cluster.

Jobs can be unit-tested using `py.test`. For a given job, create a corresponding job in `tests/jobs/` folder and add tests that relate to the specific business logic in this job. See [tests/jobs/ex1_frameworked_job_test.py](tests/jobs/ex1_frameworked_job_test.py)for an example.

## Unit-testing
... is done using `py.test`. Run them with:

		yaetos launch_docker_bash
		# From inside the docker container
    py.test tests/*  # for all tests
    py.test tests/jobs/examples/ex1_frameworked_job.py  # for tests for a specific file

## Installation instructions

To install the library and create a folder with all necessary files and folders:

    pip install yaetos
    cd /path/to/an/empty/folder/that/will/contain/pipeline/code
    yaetos setup  # to create sub-folders and setup framework files.

The setup comes with a docker environment with all libraries necessary (python and spark). It also comes with sample jobs pulling public data. To test running one of the sample job locally, in docker:

		yaetos run_dockerized jobs/examples/ex1_frameworked_job.py --dependencies

The docker container is setup to share the current folder with the host, so ETL jobs can be written from your host machine, using any IDE, and run from the container directly.

To get jobs executed and/or scheduled in AWS, You need to:
 * fill AWS parameters in `conf/config.cfg`.
 * have `~/.aws/` folder setup to give access to AWS secret keys. If not, run `pip install  awscli`, and `aws configure`.

To check running the same job in the cloud works:

 		yaetos run_dockerized jobs/examples/ex1_frameworked_job.py --dependencies --deploy=EMR

The status of the job can be monitored in AWS in the EMR section.

## Potential improvements

 * more unit-testing
 * integration with other scheduling tools (airflow...)
 * integration with other resource provisioning tools (kubernetes...)
 * automatic pulling/pushing data from s3 to local (sampled) for local development
 * easier dataset reconciliation
 * ...

Lots of room for improvement. Contributions welcome.
