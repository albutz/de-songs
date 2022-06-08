"""Functions to read local files."""
from pathlib import Path
from typing import List

import h5py
import numpy as np
import pandas as pd


def get_file_paths(data_dir: Path, pattern: str = "*.h5") -> List[Path]:
    """Recursive pattern matching in the data directory.

    Args:
        data_dir (Path): Path to directory to seach recursively.
        pattern (str): Pattern used in the search.

    Returns:
        List[Path]: Paths of matching files
    """
    return [file_path.resolve() for file_path in data_dir.rglob(pattern)]


def read_h5(file_path: Path) -> pd.DataFrame:
    """Read a single h5 file and convert it to a DataFrame.

    Args:
        file_path (Path): Path to an h5 file.

    Returns:
        pd.DataFrame: Data from h5 file as a DataFrame.
    """
    df_list = []
    items = ["analysis/songs", "metadata/songs", "musicbrainz/songs"]

    for item in items:
        with h5py.File(file_path, "r") as hf:
            df_list.append(pd.DataFrame(np.array(hf.get(item))))

    return pd.concat(df_list, axis="columns")
