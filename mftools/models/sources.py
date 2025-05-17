"""Models for data sources."""

import abc
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl


class Ticker:
    """Class to hold ticker information."""

    polars_schema = pl.Schema(
        {
            "symbol": pl.String(),
            "name": pl.String(),
            "isin": pl.String(),
        }
    )

    def __init__(
        self,
        symbol: str,
        name: str,
        isin: Optional[str],
    ):
        """Initialize the ticker."""
        self.symbol = symbol
        self.name = name
        self.isin = isin

    def __repr__(self):
        """Return a string representation of the ticker."""
        return f'Ticker(symbol="{self.symbol}", name="{self.name}", isin="{self.isin}")'

    def to_dict(self):
        """Convert the ticker to a dictionary."""
        return {
            "symbol": self.symbol,
            "name": self.name,
            "isin": self.isin,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Ticker":
        """Create a Ticker instance from a dictionary."""
        return cls(
            symbol=data.get("symbol"),
            name=data.get("name"),
            isin=data.get("isin"),
        )


class PriceData:
    """Class to hold price data."""

    def __init__(self, date: date, price: Decimal):
        """Initialize the price data."""
        self.date = date
        self.price = price

    def __repr__(self):
        """Return a string representation of the price data."""
        return f'PriceData(date="{self.date}", price={self.price})'


class SourceInfo:
    """Class to hold source information."""

    def __init__(
        self,
        name: str,
        description: str,
        key: str,
        ticker_refresh_interval: timedelta = timedelta(days=1),
        data_refresh_interval: timedelta = timedelta(days=1),
    ):
        """Initialize the source info."""
        self.name = name
        self.description = description
        self.key = key
        self.ticker_refresh_interval = ticker_refresh_interval
        self.data_refresh_interval = data_refresh_interval

    def __repr__(self):
        """Return a string representation of the source info."""
        return f'SourceInfo(name="{self.name}", description="{self.description}", key="{self.key}", ticker_refresh_interval={self.ticker_refresh_interval}, data_refresh_interval={self.data_refresh_interval})'


class Source(abc.ABC):
    """Base class for all data sources."""

    def __init__(self):
        """Initialize the source."""
        super().__init__()

    @abc.abstractmethod
    def download_historical_data(
        self,
        raw_file_path: Path,
        start_date: date,
        end_date: date,
        tickers: Optional[List[str]] = None,
    ) -> bool:
        """Download historical data."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abc.abstractmethod
    def download_latest_data(
        self, raw_file_path: Path, tickers: Optional[List[str]] = None
    ) -> bool:
        """Download latest data."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abc.abstractmethod
    def process_data(self, raw_file_path: Path) -> Union[pl.DataFrame, pd.DataFrame]:
        """Process the downloaded data."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abc.abstractmethod
    def get_tickers(self) -> Union[List[Ticker], pl.DataFrame, pd.DataFrame]:
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
