"""
Helper functions for pandas as engine.
"""

import pandas as pd
from pathlib import Path
from yaetos.logger import setup_logging
logger = setup_logging('pandas')

def create_subfolders(path):
    """Create subfolders if needed. Can take a path with a file."""
    # output_file = 'my_file.csv'
    # path_without_leaf = path.split('/')
    # path_without_leaf = path_without_leaf[:-1]
    output_dir = Path(path).parent
    # import ipdb; ipdb.set_trace()

    output_dir.mkdir(parents=True, exist_ok=True)
    # df.to_csv(output_dir / output_file)

def save_pandas(df, path):
    create_subfolders(path)
    df.to_csv(path)


if __name__ == '__main__':
    pass
