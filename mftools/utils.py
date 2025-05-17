"""Utility functions for mftools."""

from pathlib import Path
import pandas as pd
import platformdirs
from typing import Any, Dict, Iterable, NamedTuple, Optional, TypeVar, Union, List
import polars as pl

from mftools.models.helpers import ReturnFormat


def get_tickers_dir() -> Path:
    """Get the directory for tickers."""
    return Path(platformdirs.user_data_dir("mftools")).joinpath("tickers")


def handle_input(
    data: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, Iterable[NamedTuple]],
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
    data: Union[pl.DataFrame, pl.LazyFrame],
    format: Union[ReturnFormat, str],
) -> Union[Dict[str, List[Any]], pl.DataFrame, pl.LazyFrame, pd.DataFrame, str]:
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


PolarsFrame = TypeVar("PolarsFrame", pl.DataFrame, pl.LazyFrame)


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
