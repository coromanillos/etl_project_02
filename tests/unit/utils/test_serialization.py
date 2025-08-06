# tests/test_serialization.py

import pandas as pd
import tempfile
from pathlib import Path
from src.utils.serialization import save_dataframe_to_parquet

def test_save_dataframe_to_parquet(tmp_path):
    # Arrange
    df = pd.DataFrame({"a": [1, 2, 3]})
    file_path = tmp_path / "nested" / "test_output.parquet"

    # Act
    save_dataframe_to_parquet(df, str(file_path))

    # Assert
    assert file_path.exists()
    df_loaded = pd.read_parquet(file_path)
    pd.testing.assert_frame_equal(df, df_loaded)
