# Before executing, version needs to be incremented in setup.py
# For testing purpose,
# - option 1: send package to testing pypi server: https://packaging.python.org/guides/using-testpypi/
# - option 2: "cd (1 level above repo); pip install ./yaetos" will install directly without going through server. Will install shims "yaetos_install"


rm -r dist/
rm -r yaetos.egg-info/
rm -r build/

# Build
python -m build --wheel  # requires "pip install build"

# Ship to pip servers.
twine check dist/*  #  requires "pip install twine"
twine upload dist/*
