# test_serialization.py

import pandas as pd
import pytest
from src.utils.serialization import serialize_df, deserialize_df

@pytest.fixture
def simple_dataframe():
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

def test_serialize_deserialize_round_trip(simple_dataframe):
    blob = serialize_df(simple_dataframe)
    result_df = deserialize_df(blob)
    pd.testing.assert_frame_equal(simple_dataframe, result_df)

def test_serialize_invalid_input():
    with pytest.raises(TypeError):  # Expecting a more specific error
        serialize_df("not a dataframe")

def test_deserialize_invalid_blob():
    with pytest.raises(Exception):  # If you know the expected error, replace `Exception`
        deserialize_df(b"not parquet data")
