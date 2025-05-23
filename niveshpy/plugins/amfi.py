"""A built-in plugin for NiveshPy that provides AMFI as a source."""

from datetime import timedelta
import logging
import niveshpy
from niveshpy.models.base import SourceConfig, SourceInfo, SourceStrategy
from niveshpy.models.plugins import Plugin, PluginInfo
from niveshpy.models.sources import Source
import polars as pl

logger = logging.getLogger(__name__)


class AMFIPlugin(Plugin):
    """AMFI Plugin for NiveshPy."""

    plugin_info = PluginInfo(
        name="AMFI",
        description="AMFI plugin for NiveshPy",
        version=niveshpy.__version__,
        author="Yashovardhan Dhanania",
        author_email="",
    )

    def __init__(self) -> None:
        """Initialize the AMFI plugin."""
        super().__init__()
        self.sources = [AMFISource()]

    @classmethod
    def get_info(cls) -> PluginInfo:
        """Return plugin information."""
        return cls.plugin_info

    def get_sources(self):
        """Return a list of sources for the plugin."""
        # Here you would return a list of sources that the plugin provides.
        # For example:
        return self.sources


class AMFISource(Source):
    """AMFI Source for NiveshPy."""

    LATEST_URL = "http://amfiindia.com/spages/NAVAll.txt"
    HISTORICAL_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={frm_dt}&todt={to_dt}"

    def __init__(self) -> None:
        """Initialize the AMFI source."""
        super().__init__()
        # Initialize any necessary attributes here.

    def get_quotes(self, *_, start_date=None, end_date=None):
        """Get quotes for the all tickers."""
        url = self.LATEST_URL
        if start_date and end_date:
            url = self.HISTORICAL_URL.format(
                frm_dt=start_date.strftime("%d-%b-%Y"),
                to_dt=end_date.strftime("%d-%b-%Y"),
            )
        elif start_date:
            url = self.HISTORICAL_URL.format(
                frm_dt=start_date.strftime("%d-%b-%Y"),
                to_dt=start_date.strftime("%d-%b-%Y"),
            )
        elif end_date:
            url = self.HISTORICAL_URL.format(
                frm_dt=end_date.strftime("%d-%b-%Y"),
                to_dt=end_date.strftime("%d-%b-%Y"),
            )

        df = pl.read_csv(
            url,
            separator=";",
            null_values=["N.A.", "-"],
            infer_schema=False,
        )
        df = df.drop_nulls(subset=["Date"])
        df = df.select(
            pl.col("Scheme Code").alias("symbol").cast(pl.String()),
            pl.col("Date").alias("date").str.strptime(pl.Date, "%d-%b-%Y"),
            pl.col("Net Asset Value").alias("price").cast(pl.Decimal(None, 4)),
        )
        return df

    def get_tickers(self):
        """Get the list of tickers."""
        try:
            df = pl.scan_csv(
                self.LATEST_URL,
                separator=";",
                null_values=["N.A.", "-"],
                infer_schema=False,
            )
            df = df.drop_nulls(subset=["Date"])
            return df.select(
                pl.col("Scheme Code").alias("symbol"),
                pl.col("Scheme Name").alias("name"),
                pl.coalesce(pl.col("^ISIN .*$")).alias("isin"),
            )
        except Exception:
            logger.exception("Failed to get tickers")
            return []

    @classmethod
    def get_source_key(cls):
        """Return the source key."""
        return "amfi"

    @classmethod
    def get_source_info(cls):
        """Return source information."""
        return SourceInfo(
            name="Mutual Fund India",
            description="Data source for all Indian mutual funds, sourced from AMFI.",
            key=AMFISource.get_source_key(),
            version=1,
        )

    @classmethod
    def get_source_config(cls):
        """Return source configuration."""
        return SourceConfig(
            ticker_refresh_interval=timedelta(days=7),
            data_refresh_interval=timedelta(days=1),
            data_group_period=timedelta(days=30),
            source_strategy=SourceStrategy.ALL_TICKERS | SourceStrategy.SINGLE_QUOTE,
        )


def register_plugin() -> AMFIPlugin:
    """Register the plugin with NiveshPy."""
    return AMFIPlugin()
