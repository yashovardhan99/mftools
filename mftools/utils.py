"""Utility functions for mftools."""

from pathlib import Path
import pandas as pd
import platformdirs
from typing import Any, NamedTuple, Optional, Union
import polars as pl

from mftools.models.helpers import ReturnFormat
from mftools.models.types import (
    MFToolsIterable,
    PolarsFrameType,
    PolarsFrame,
)


def get_tickers_dir() -> Path:
    """Get the directory for tickers."""
    return platformdirs.user_data_path("mftools").joinpath("tickers")


def get_quotes_dir() -> Path:
    """Get the directory for quotes."""
    return platformdirs.user_data_path("mftools").joinpath("quotes")


def handle_input(
    data: MFToolsIterable,
    schema: Optional[pl.Schema] = None,
) -> pl.LazyFrame:
    """Handle input data and convert it to a Polars LazyFrame."""
    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        return data.lazy()
    elif isinstance(data, pd.DataFrame):
        return pl.from_pandas(data, schema_overrides=schema).lazy()
    else:
        return pl.from_dicts(map(NamedTuple._asdict, data), schema=schema).lazy()


def format_output(
    data: PolarsFrameType,
    format: Union[ReturnFormat, str],
) -> Union[dict[str, list[Any]], pl.DataFrame, pl.LazyFrame, pd.DataFrame, str]:
    """Format the output based on the specified format."""
    data = data.lazy()
    if isinstance(format, str):
        format = ReturnFormat(format)

    if format == ReturnFormat.DICT:
        return data.collect().to_dict(as_series=False)
    elif format == ReturnFormat.PL_DATAFRAME:
        return data.collect()
    elif format == ReturnFormat.PL_LAZYFRAME:
        return data
    elif format == ReturnFormat.PD_DATAFRAME:
        return data.collect().to_pandas()
    elif format == ReturnFormat.JSON:
        return data.collect().write_json()
    elif format == ReturnFormat.CSV:
        return data.collect().write_csv()
    else:
        raise ValueError(f"Unsupported format: {format}")


def apply_filters(
    frame: PolarsFrame,
    source_keys: Optional[list[str]],
    filters: Optional[dict[str, list[str]]],
    schema: Optional[pl.Schema] = None,
) -> PolarsFrame:
    """Filter records based on source keys and other filters.

    All filters are combined using OR. The keys of the dictionary are
    the column names and the values are lists of values to filter by.

    Example:
    >>>     filters = {
    ...         "symbol": ["0500209", "500210"],
    ...         "name": ["UTI Nifty Next 50 Index Fund - Direct"]
    ...     }

    This will return only the records that match the specified symbol or name.
    If filters is None, all records are returned.
    """
    if source_keys:
        frame = frame.filter(pl.col("source_key").is_in(source_keys))
    if filters:
        columns = schema.names() if schema else frame.collect_schema().names()
        expressions = None
        for column, values in filters.items():
            if column in columns:
                if expressions is None:
                    expressions = pl.lit(False)
                expressions = expressions | pl.col(column).is_in(values)
        if expressions is not None:
            frame = frame.filter(expressions)
    return frame


def save_tickers(tickers: PolarsFrameType) -> None:
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


def save_all_tickers_availability(availability: PolarsFrameType) -> None:
    """Save availability for all sources with ALL_TICKERS strategy."""
    file_path = get_quotes_dir().joinpath("all_tickers_availability.parquet")
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)
    availability.lazy().collect().write_parquet(file_path)


def load_all_tickers_availability() -> pl.LazyFrame:
    """Load availability for all sources with ALL_TICKERS strategy."""
    file_path = get_quotes_dir().joinpath("all_tickers_availability.parquet")
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist.")
    return pl.scan_parquet(file_path)


def save_default_availability(availability: PolarsFrameType, source_key: str) -> None:
    """Save availability for a source with DEFAULT strategy."""
    file_path = get_quotes_dir().joinpath(f"default_availability_{source_key}.parquet")
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)
    availability.lazy().collect().write_parquet(file_path)


def load_default_availability(source_key: str) -> pl.LazyFrame:
    """Load availability from a parquet file."""
    file_path = get_quotes_dir().joinpath(f"default_availability_{source_key}.parquet")
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist.")
    return pl.scan_parquet(file_path)


def save_quotes(quotes: PolarsFrameType, source_key: str) -> None:
    """Save quotes to a parquet file."""
    file_path = get_quotes_dir().joinpath(f"quotes_{source_key}.parquet")
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)
    quotes.lazy().collect().write_parquet(file_path)
