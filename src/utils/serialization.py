###########################################
# Name: serialization.py
# Author: Christopher O. Romanillos
# Description: Efficiently pass data between scripts 
# Date: 08/04/25
###########################################

import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)

def serialize_df(df: pd.DataFrame) -> bytes:
    """
    Serialize a DataFrame into Parquet format and return as bytes.
    """
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        logger.debug("DataFrame serialized to Parquet successfully.")
        return buffer.getvalue()
    except Exception as e:
        logger.error(f"Serialization failed: {e}")
        raise

def deserialize_df(blob: bytes) -> pd.DataFrame:
    """
    Deserialize bytes back into a DataFrame from Parquet format.
    """
    try:
        buffer = io.BytesIO(blob)
        df = pd.read_parquet(buffer)
        logger.debug("DataFrame deserialized from Parquet successfully.")
        return df
    except Exception as e:
        logger.error(f"Deserialization failed: {e}")
        raise
