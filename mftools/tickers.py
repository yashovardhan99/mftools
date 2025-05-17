"""Tickers related functions."""

from typing import Union, TypeVar
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


Frame = TypeVar("PolarsFrame", pl.DataFrame, pl.LazyFrame)


def filter_tickers(
    tickers: Frame,
    source_keys: list[str] | None = None,
    filters: dict[str, list[str]] | None = None,
) -> Frame:
    """Filter tickers based on source keys and other filters."""
    if source_keys:
        tickers = tickers.filter(pl.col("source_key").is_in(source_keys))
    if filters:
        expressions = None
        for column, values in filters.items():
            if column in tickers.columns:
                if expressions is None:
                    expressions = pl.lit(False)
                expressions = expressions | pl.col(column).is_in(values)
        if expressions is not None:
            tickers = tickers.filter(expressions)
    return tickers
