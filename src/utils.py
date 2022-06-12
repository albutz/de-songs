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
    val = ser.to_numpy()[0]
    val = None if pd.isnull(val) else val
    return val


def encode_str(ser: pd.Series) -> str | None:
    """Encode byte strings.

    Args:
        ser: A pandas series.

    Returns:
        String value.
    """
    val = get_scalar(ser)

    if val is None:
        return val

    if isinstance(val, bytes):
        val = val.decode("utf8")

    if len(val) == 0:
        val = None

    return val


def cast_numeric(ser: pd.Series) -> int | float | None:
    """Cast numpy numerics.

    Args:
        ser: A pandas series.

    Returns:
        Numeric value.
    """
    val = get_scalar(ser)

    if val is None:
        return val

    if isinstance(val, np.integer):
        val = int(val)
    elif isinstance(val, np.floating):
        val = float(val)

    return val
