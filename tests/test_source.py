"""Test source related functionality."""

from datetime import timedelta, date
from collections.abc import Iterable
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
                open=100.0,
                high=110.0,
                low=90.0,
                close=105.0,
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
