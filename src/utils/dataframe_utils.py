#################################################################################
# Name: dataframe_utils.py
# Author: Christopher O. Romanillos
# Description: Generic pandas DataFrame utilities
# Date: 08/21/25
#################################################################################

import pandas as pd


def cast_numeric_and_datetime(
    df: pd.DataFrame,
    numeric_cols: list[str],
    datetime_cols: list[str],
    logger
) -> pd.DataFrame:
    """Cast specified columns to numeric and datetime formats."""
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            logger.debug(f"Converted {col} to numeric")

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            logger.debug(f"Converted {col} to datetime")

    return df
