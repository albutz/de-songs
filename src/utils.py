"""Helper functions."""
from typing import Any

import numpy as np
import pandas as pd


def get_scalar(ser: pd.Series) -> Any:
    """Get the scalar value from a pandas Series of length 1.

    Args:
        ser: A pandas series.

    Returns:
        Scalar value.
    """
    return ser.to_numpy()[0]


def encode_str(ser: pd.Series) -> str:
    """Encode byte strings.

    Args:
        ser: A pandas series.

    Returns:
        String value.
    """
    val = get_scalar(ser)
    if isinstance(val, bytes):
        val = val.decode("utf8")
    return val


def cast_numeric(ser: pd.Series) -> int | float:
    """Cast numpy numerics.

    Args:
        ser: A pandas series.

    Returns:
        Numeric value.
    """
    val = get_scalar(ser)
    if isinstance(val, np.integer):
        val = int(val)
    elif isinstance(val, np.floating):
        val = float(val)
    return val
