rm -r dist/
rm -r yaetos.egg-info/
rm -r build/

# Build
python -m build --wheel  # requires "pip install build"

# Ship to pip servers.
twine check dist/*  #  requires "pip install twine"
twine upload dist/*
