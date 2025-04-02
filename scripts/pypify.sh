# Before executing, version needs to be incremented in ./setup.py and in ./yaetos/scripts/Dockerfile_external.
# To see latest published version https://pypi.org/project/yaetos/
# To see local version : pip show yaetos
# For testing purpose,
# - option 1: send package to testing pypi server: https://packaging.python.org/guides/using-testpypi/
# - option 2: "cd (1 level above repo); pip install ./yaetos" will install directly without going through server. Will install shims "yaetos_install"

# Copy files needed in ./yaetos/scripts/ to check later that they are in git.
# They need to be also in sync with list in yaetos/scripts/yaetos_cmdline.py
# TODO: find better way to deal with this, avoiding file duplication
## Config files
cp conf/connections.cfg.example yaetos/scripts/copy/connections.cfg.example
cp conf/aws_config.cfg.example yaetos/scripts/copy/aws_config.cfg.example
## Jobs
cp jobs/generic/copy_job.py yaetos/libs/generic_jobs/copy_job.py
cp jobs/generic/copy_raw_job.py yaetos/libs/generic_jobs/copy_raw_job.py
cp jobs/generic/deployer.py yaetos/libs/generic_jobs/deployer.py
cp jobs/generic/dummy_job.py yaetos/libs/generic_jobs/dummy_job.py
cp jobs/generic/launcher.py yaetos/libs/generic_jobs/launcher.py
cp jobs/generic/sql_spark_job.py yaetos/libs/generic_jobs/sql_spark_job.py
cp jobs/generic/sql_pandas_job.py yaetos/libs/generic_jobs/sql_pandas_job.py
cp jobs/generic/compare_job.py yaetos/libs/generic_jobs/compare_job.py
cp jobs/generic/list_files_job.py yaetos/libs/generic_jobs/list_files_job.py
cp jobs/examples/ex0_extraction_job.py yaetos/scripts/copy/ex0_extraction_job.py
cp jobs/examples/ex1_sql_job.sql yaetos/scripts/copy/ex1_sql_job.sql
cp jobs/examples/ex7_pandas_job.py yaetos/scripts/copy/ex1_pandas_api_job.py
cp jobs/examples/ex1_frameworked_job.py yaetos/scripts/copy/ex1_spark_api_job.py
cp tests/jobs/examples/ex1_sql_pandas_job_test.py yaetos/scripts/copy/ex1_sql_pandas_job_test.py
cp tests/jobs/examples/ex1_sql_spark_job_test.py yaetos/scripts/copy/ex1_sql_spark_job_test.py
## Other files not to be copied exactly but close (to be checked before running this script):
#  - cp yaetos/libs/pytest_utils/conftest.py tests/conftest.py  # yaetos/libs/pytest_utils should be considered master copy and tests/conftest.py should be removed (TODO), although currently tests/conftest.py may be changed directly for dev reasons.
#  - cp conf/requirements_extra.txt yaetos/scripts/copy/requirements_extra.txt # yaetos may have libs to run jobs not included in yaetos_jobs, so not needed there.
#  - cp .github/workflows/pythonapp.yml yaetos/scripts/github_pythonapp.yml
#  - cp Dockerfile yaetos/scripts/copy/Dockerfile_external
#  - cp conf/jobs_metadata.yml yaetos/scripts/copy/jobs_metadata_external.yml
#  - cp tests/jobs/examples/ex7_pandas_job_test.py yaetos/scripts/copy/ex1_pandas_api_job_test.py # need to update import path after copy. TODO: uncomment when yaetos repo job names match with pip installed job names.
#  - cp tests/jobs/examples/ex1_frameworked_job_test.py yaetos/scripts/copy/ex1_spark_api_job_test.py # need to update import path after copy. TODO: uncomment when yaetos repo job names match with pip installed job names.


rm -r dist/
rm -r yaetos.egg-info/
rm -r build/

# Build
# python -m build --wheel  # requires "pip install build"  # old way, stopped working in last months.
python setup.py sdist bdist_wheel  # new way

# Ship to pip servers.
twine check dist/*  #  requires "pip install twine"
twine upload dist/*
