# Setup for run in local mode
# Assumes spark (2.1.0) is already setup locally or in docker image

mkdir -p libs
cd libs
git clone https://github.schibsted.io/arthur-prevot/python_db_connectors.git
git clone https://github.schibsted.io/arthur-prevot/local_analysis_tool.git

## copy credentials.cfg.example to credentials.cfg and fill it.
