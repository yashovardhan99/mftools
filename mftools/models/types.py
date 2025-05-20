"""Type aliases for MFTools models."""

from typing import TypeVar, Union
from collections.abc import Iterable

import polars as pl
import pandas as pd

from mftools.models.base import Quote, SourceConfig, SourceInfo, Ticker

# Type Aliases
PolarsFrameType = Union[pl.DataFrame, pl.LazyFrame]
Frame = Union[PolarsFrameType, pd.DataFrame]

MFToolsType = Union[Ticker, Quote, SourceInfo, SourceConfig]

QuotesIterable = Union[Iterable[Quote], Frame]
TickersIterable = Union[Iterable[Ticker], Frame]

MFToolsIterable = Union[Frame, Iterable[MFToolsType]]

MFToolsOutputType = Union[
    dict[str, list], pl.DataFrame, pl.LazyFrame, pd.DataFrame, str
]

# Type Variables
PolarsFrame = TypeVar("PolarsFrame", pl.DataFrame, pl.LazyFrame)
