"""Type aliases for NiveshPy models."""

from typing import TypeVar, Union
from collections.abc import Iterable

import polars as pl
import pandas as pd

from niveshpy.models.base import Quote, SourceConfig, SourceInfo, Ticker

# Type Aliases
PolarsFrameType = Union[pl.DataFrame, pl.LazyFrame]
Frame = Union[PolarsFrameType, pd.DataFrame]

NiveshPyType = Union[Ticker, Quote, SourceInfo, SourceConfig]

QuotesIterable = Union[Iterable[Quote], Frame]
TickersIterable = Union[Iterable[Ticker], Frame]

NiveshPyIterable = Union[Frame, Iterable[NiveshPyType]]

NiveshPyOutputType = Union[
    dict[str, list], pl.DataFrame, pl.LazyFrame, pd.DataFrame, str
]

# Type Variables
PolarsFrame = TypeVar("PolarsFrame", pl.DataFrame, pl.LazyFrame)
