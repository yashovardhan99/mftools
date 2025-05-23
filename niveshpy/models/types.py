"""Type aliases for NiveshPy models.

This module is designed to work with static type checkers and as such
imports optional depedencies like pandas. Avoid importing this module
directly, instead import this module inside a `if TYPE_CHECKING` block.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TypeVar, Union

import pandas as pd
import polars as pl

from niveshpy.models.base import OHLC, Quote, SourceConfig, SourceInfo, Ticker

# Type Aliases
PolarsFrameType = Union[pl.DataFrame, pl.LazyFrame]

NiveshPyType = Union[Ticker, Quote, OHLC, SourceInfo, SourceConfig]

QuotesIterable = Union[Iterable[Quote], Iterable[OHLC], PolarsFrameType]
TickersIterable = Union[Iterable[Ticker], PolarsFrameType]

NiveshPyIterable = Union[PolarsFrameType, Iterable[NiveshPyType]]

NiveshPyOutputType = Union[
    dict[str, list], pl.DataFrame, pl.LazyFrame, pd.DataFrame, str
]

# Type Variables
PolarsFrame = TypeVar("PolarsFrame", pl.DataFrame, pl.LazyFrame)
