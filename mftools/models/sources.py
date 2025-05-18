"""Models for data sources."""

import abc
from datetime import date, timedelta
from decimal import Decimal
from enum import Flag, auto
from typing import Optional, NamedTuple

import polars as pl

from mftools.models.types import QuotesIterable, TickersIterable


class Ticker(NamedTuple):
    """Class to hold ticker information."""

    symbol: str
    name: str
    isin: Optional[str]

    @classmethod
    def get_polars_schema(cls) -> pl.Schema:
        """Get the Polars schema for the Ticker class."""
        return pl.Schema(
            {
                "symbol": pl.String(),
                "name": pl.String(),
                "isin": pl.String(),
            }
        )


class Quote(NamedTuple):
    """Class to hold price data."""

    symbol: str
    date: date
    price: Decimal

    @classmethod
    def get_polars_schema(cls) -> pl.Schema:
        """Get the Polars schema for the Quote class."""
        return pl.Schema(
            {
                "symbol": pl.String(),
                "date": pl.Date(),
                "price": pl.Decimal(scale=4),
            }
        )


class SourceInfo(NamedTuple):
    """Class to hold source information."""

    name: str
    description: str
    key: str
    version: int
    """This will later add support for source migrations."""


class SourceStrategy(Flag):
    """Enum for source strategies.

    This enum defines the different strategies for data sources.
    Strategies can be combined using bitwise OR.
    These help in determining how to fetch and store data from the source.
    For example, if the source only supports fetching data for all tickers at a time,
    MFTools will store the data and use them in the future automatically.
    """

    DEFAULT = 0
    """Use this when the source does not require any special strategy.
    
    Default strategy:
    - The source only fetches data for the provided tickers (or all tickers if none are provided)."""

    ALL_TICKERS = auto()
    """The source fetches data for all tickers at once.
    Used for sources that do not support fetching data only for a list of provided tickers.
    """


class SourceConfig(NamedTuple):
    """Class to hold source configuration."""

    ticker_refresh_interval: Optional[timedelta] = None
    """The time interval at which the source can be checked for new tickers.
    
    If this value is None, the source will not be checked for new tickers.
    Default is None."""
    data_refresh_interval: timedelta = timedelta(days=1)
    """The time interval at which the source can be checked for new data.
    
    Note that this only applies to new data. Historical data, once fetched,
    will not be fetched again.
    
    This frequency will be ticker-specific unless the source uses the `ALL_TICKERS` strategy,
    in which case it will be source-specific."""
    data_group_period: Optional[timedelta] = None
    """The time period for which data can be grouped at source.
    
    This is used to limit the amount of calls made to the source.
    For example, if the source can return data for 1 month at a time,
    this should be set to 30 days.

    If this value is None, the data will not be grouped.
    
    This value will be passed to `polars.DataFrame.group_by_dynamic`
    to group the data by the specified time period.

    Default is None.
    """
    source_strategy: SourceStrategy = SourceStrategy.DEFAULT
    """The strategy to use for the source. Multiple strategies can be combined using bitwise OR.
    This is used to determine how to fetch and store data from the source."""


class Source(abc.ABC):
    """Base class for all data sources."""

    def __init__(self) -> None:
        """Initialize the source."""
        super().__init__()

    @abc.abstractmethod
    def get_quotes(
        self,
        *tickers: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> QuotesIterable:
        """Get the quotes for the given tickers.

        Args:
            *tickers (str): The list of tickers to get quotes for. If empty, all tickers will be fetched.
            start_date (Optional[date]): The start date for the quotes. If none, the source should return the latest data.
            end_date (Optional[date]): The end date for the quotes. If none, the source should return the latest data.

            The source can return data for all tickers irrespective of the provided tickers
            if it uses the `ALL_TICKERS` strategy.

        Note:
        - If both start_date and end_date are None, the source should return the latest data.
        - If only one date is provided, the source should return data for that date.
        - Date range would never exceed the `data_group_period` specified in the source config.

        Returns:
            An iterable of Quote objects or a Polars DataFrame or a Pandas DataFrame.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abc.abstractmethod
    def get_tickers(self) -> TickersIterable:
        """Get the list of tickers."""
        raise NotImplementedError("Subclasses must implement this method.")

    @classmethod
    @abc.abstractmethod
    def get_source_key(cls) -> str:
        """Get a unique key for this source."""
        raise NotImplementedError("Subclasses must implement this method.")

    @classmethod
    @abc.abstractmethod
    def get_source_info(cls) -> SourceInfo:
        """Get source information."""
        raise NotImplementedError("Subclasses must implement this method.")

    @classmethod
    @abc.abstractmethod
    def get_source_config(cls) -> SourceConfig:
        """Get source configuration."""
        raise NotImplementedError("Subclasses must implement this method.")
