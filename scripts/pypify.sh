# Before executing, version needs to be incremented in ./setup.py and in ./yaetos/scripts/Dockerfile_external.
# To see latest published version https://pypi.org/project/yaetos/
# To see local version : pip show yaetos
# For testing purpose,
# - option 1: send package to testing pypi server: https://packaging.python.org/guides/using-testpypi/
# - option 2: "cd (1 level above repo); pip install ./yaetos" will install directly without going through server. Will install shims "yaetos_install"

# Copy files needed in ./yaetos/scripts/ to check later that they are in git.
# They need to be also in sync with list in yaetos/scripts/yaetos_cmdline.py
# TODO: find better way to deal with this, avoiding file duplication
# TODO: put duplicated files in subfolder "copy" to make it explicit
cp conf/connections.cfg.example yaetos/scripts/copy/connections.cfg.example
cp conf/aws_config.cfg.example yaetos/scripts/copy/aws_config.cfg.example
cp .github/workflows/pythonapp.yml yaetos/scripts/copy/github_pythonapp.yml
## Jobs
cp jobs/generic/copy_job.py yaetos/libs/generic_jobs/copy_job.py
cp jobs/generic/deployer.py yaetos/libs/generic_jobs/deployer.py
cp jobs/generic/dummy_job.py yaetos/libs/generic_jobs/dummy_job.py
cp jobs/generic/launcher.py yaetos/libs/generic_jobs/launcher.py
cp jobs/examples/ex0_extraction_job.py yaetos/scripts/copy/ex0_extraction_job.py
cp jobs/examples/ex1_frameworked_job.py yaetos/scripts/copy/ex1_frameworked_job.py
cp jobs/examples/ex1_full_sql_job.sql yaetos/scripts/copy/ex1_full_sql_job.sql
cp tests/jobs/examples/ex1_full_sql_job_test.py yaetos/scripts/copy/ex1_full_sql_job_test.py

rm -r dist/
rm -r yaetos.egg-info/
rm -r build/

# Build
python -m build --wheel  # requires "pip install build"

# Ship to pip servers.
twine check dist/*  #  requires "pip install twine"
twine upload dist/*
