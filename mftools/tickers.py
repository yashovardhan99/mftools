"""Tickers related functions."""

from typing import Union
import polars as pl
from mftools.utils import get_tickers_dir


def save_tickers(tickers: Union[pl.DataFrame, pl.LazyFrame]) -> None:
    """Save tickers to a parquet file partitioned by source_key."""
    file_path = get_tickers_dir().joinpath("tickers.parquet")
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)
    tickers.lazy().collect().write_parquet(file_path)


def load_tickers() -> pl.LazyFrame:
    """Load tickers from a parquet file."""
    file_path = get_tickers_dir().joinpath("tickers.parquet")
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist.")
    return pl.scan_parquet(file_path)
