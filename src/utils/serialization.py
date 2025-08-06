###########################################
# Name: serialization.py
# Author: Christopher O. Romanillos
# Description: Efficiently pass data between scripts 
# Date: 08/04/25
###########################################

import pandas as pd
from pathlib import Path
import io

def save_dataframe_to_parquet(df: pd.DataFrame, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
