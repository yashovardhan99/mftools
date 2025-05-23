"""Test source related functionality."""

from datetime import datetime, timedelta, date
from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path
import polars as pl

import pytest
from niveshpy.main import Nivesh
from niveshpy.models.plugins import Plugin, PluginInfo
from niveshpy.models.sources import Source
from niveshpy.models.base import OHLC, SourceInfo, SourceConfig, SourceStrategy, Ticker


class DummySourcePlugin(Plugin):
    """Test plugin for NiveshPy."""

    def __init__(self):
        """Initialize the Test plugin."""
        super().__init__()
        self.sources = [DummySource()]

    @classmethod
    def get_info(cls):
        """Return plugin information."""
        return PluginInfo(
            name="TestPlugin",
            description="A test plugin for NiveshPy",
            version="1.0.0",
            author="Test Author",
            author_email="",
        )

    def get_sources(self):
        """Return a list of sources for the plugin."""
        return [DummySource()]


class DummySource(Source):
    """Test source for NiveshPy."""

    def __init__(self):
        """Initialize the Test source."""
        super().__init__()

    def get_quotes(self, *args, **kwargs):
        """Get quotes for the all tickers."""
        return [
            OHLC(
                symbol="dummy_ticker_1",
                date=date(2023, 10, 1),
                open=Decimal(100.0),
                high=Decimal(110.0),
                low=Decimal(90.0),
                close=Decimal(105.0),
            )
        ]

    def get_tickers(self):
        """Get the list of tickers."""
        return [
            Ticker("dummy_ticker_1", "Dummy Ticker 1", "ISIN0000"),
        ]

    @classmethod
    def get_source_key(cls):
        """Get a unique key for this source."""
        return "dummy_source"

    @classmethod
    def get_source_info(cls) -> SourceInfo:
        """Get source information."""
        return SourceInfo(
            name="DummySource",
            description="A test source for NiveshPy",
            version=1,
            key="dummy_source",
        )

    @classmethod
    def get_source_config(cls):
        """Get source configuration."""
        return SourceConfig(
            ticker_refresh_interval=None,
            data_refresh_interval=timedelta(days=1),
            data_group_period=None,
            source_strategy=SourceStrategy.DEFAULT,
        )


def test_get_sources():
    """Test the get_sources method."""
    app = Nivesh()
    app.register_plugin(DummySourcePlugin())
    sources = app.get_sources()
    assert isinstance(sources, Iterable)

    sources_list = list(sources)
    assert len(sources_list) > 0
    assert isinstance(sources_list[0], SourceInfo)


def test_get_sources_specific():
    """Test the get_sources method with a specific source."""
    app = Nivesh()
    app.register_plugin(DummySourcePlugin())
    sources = app.get_sources(source_keys=["dummy_source"])
    assert isinstance(sources, Iterable)

    sources_list = list(sources)
    assert len(sources_list) == 1
    assert isinstance(sources_list[0], SourceInfo)
    assert sources_list[0].key == "dummy_source"


@pytest.fixture
def mock_get_tickers_dir(monkeypatch, tmp_path) -> Path:
    """Mock the get_tickers_dir function."""
    mock_dir = tmp_path / "tickers"
    mock_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("niveshpy.utils.get_tickers_dir", lambda: mock_dir)
    return mock_dir


@pytest.fixture
def mock_get_quotes_dir(monkeypatch, tmp_path) -> Path:
    """Mock the get_quotes_dir function."""
    mock_dir = tmp_path / "quotes"
    mock_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("niveshpy.utils.get_quotes_dir", lambda: mock_dir)
    return mock_dir


@pytest.fixture
def mock_import_local_plugins(monkeypatch):
    """Mock the import_local_plugins function."""

    def mock_func():
        return []

    monkeypatch.setattr("niveshpy.main._import_local_plugins", mock_func)


def test_get_tickers(mock_get_tickers_dir, mock_import_local_plugins):
    """Test the get_tickers method."""
    app = Nivesh()
    app.register_plugin(DummySourcePlugin())
    tickers = app.get_tickers()

    # Check if the tickers are returned correctly
    assert isinstance(tickers, dict)
    assert tickers["symbol"] == ["dummy_ticker_1"]
    assert tickers["name"] == ["Dummy Ticker 1"]
    assert tickers["isin"] == ["ISIN0000"]
    assert tickers["source_key"] == ["dummy_source"]
    assert tickers["last_updated"] is not None


def test_get_tickers_specific(mock_get_tickers_dir):
    """Test the get_tickers method."""
    app = Nivesh()
    app.register_plugin(DummySourcePlugin())
    tickers = app.get_tickers(source_keys=["dummy_source"])

    # Check if the tickers are returned correctly
    assert isinstance(tickers, dict)
    assert "dummy_ticker_1" in tickers["symbol"]

    # Check if the tickers are saved to the file
    file_path = mock_get_tickers_dir / "tickers.parquet"
    assert file_path.exists()
    assert file_path.stat().st_size > 0


def test_get_tickers_local(mock_get_tickers_dir, mock_import_local_plugins):
    """Test the get_tickers method with locally saved tickers."""
    app = Nivesh()
    app.register_plugin(DummySourcePlugin())

    df = pl.DataFrame(
        {
            "symbol": ["dummy_ticker_2"],
            "name": ["Dummy Ticker 2"],
            "isin": ["ISIN0001"],
            "source_key": ["dummy_source"],
            "last_updated": [datetime(2023, 10, 1, 12, 0, 0)],
        }
    )
    df.write_parquet(mock_get_tickers_dir / "tickers.parquet")
    tickers = app.get_tickers()

    # Check if the tickers are returned correctly
    assert isinstance(tickers, dict)
    assert tickers["symbol"] == ["dummy_ticker_2"]
    assert tickers["name"] == ["Dummy Ticker 2"]
    assert tickers["isin"] == ["ISIN0001"]
    assert tickers["source_key"] == ["dummy_source"]
    assert tickers["last_updated"] == [datetime(2023, 10, 1, 12, 0, 0)]


def test_get_quotes(
    mock_get_tickers_dir, mock_import_local_plugins, mock_get_quotes_dir
):
    """Test the get_quotes method."""
    app = Nivesh()
    app.register_plugin(DummySourcePlugin())
    quotes = app.get_quotes(start_date=date(2023, 10, 1), end_date=date(2023, 10, 1))

    # Check if the quotes are returned correctly
    assert isinstance(quotes, dict)
    assert quotes["symbol"] == ["dummy_ticker_1"]
    assert quotes["date"] == [date(2023, 10, 1)]
    assert quotes["open"] == [100.0]
    assert quotes["high"] == [110.0]
    assert quotes["low"] == [90.0]
    assert quotes["close"] == [105.0]
    assert quotes["source_key"] == ["dummy_source"]

    # Check if the quotes are saved to the file
    file_path = mock_get_quotes_dir / "quotes_dummy_source.parquet"
    assert file_path.exists()
    assert file_path.stat().st_size > 0
