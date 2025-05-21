"""Models for data sources."""

import abc
from datetime import date
from typing import Optional


from niveshpy.models.types import QuotesIterable, TickersIterable
from niveshpy.models.base import SourceInfo, SourceConfig


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

            This property should be ignored if the source uses the ALL_TICKERS strategy.

        Note:
            - If both start_date and end_date are None, the source should return the latest data.
            - If only one date is provided, the source should return data for that date.
            - Date range would never exceed the `data_group_period` specified in the source config.

        Returns:
            An iterable of Quote or OHLC objects or a Polars DataFrame or a Pandas DataFrame.
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
