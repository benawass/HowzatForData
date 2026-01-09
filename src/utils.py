"""
Utility functions for HowzatForData pipeline.
"""
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def save_to_parquet(df: pd.DataFrame, output_path: Path, description: str = "Data") -> Path:
    """
    Save DataFrame to parquet file, creating parent directories if needed.

    Args:
        df: DataFrame to save.
        output_path: Path to save the parquet file.
        description: Description for logging (e.g., "Ingested Data").

    Returns:
        The path where the file was saved.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info(f"{description} saved to {output_path} ({len(df):,} rows)")
    return output_path


def load_from_parquet(input_path: Path, description: str = "Data") -> pd.DataFrame:
    """
    Load DataFrame from parquet file.

    Args:
        input_path: Path to the parquet file.
        description: Description for logging.

    Returns:
        Loaded DataFrame.

    Raises:
        FileNotFoundError: If the parquet file doesn't exist.
    """
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"{description} not found at {input_path}. Run 'ingest' first.")
    
    df = pd.read_parquet(input_path)
    logger.info(f"{description} loaded from {input_path} ({len(df):,} rows)")
    return df
