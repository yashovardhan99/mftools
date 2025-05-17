"""A built-in plugin for MFTools that provides AMFI as a source."""

from datetime import timedelta
import logging
from pathlib import Path
import mftools
from mftools.models.plugins import Plugin, PluginInfo
from mftools.models.sources import Source, SourceInfo
import requests
import tempfile
import polars as pl

logger = logging.getLogger(__name__)


class AMFIPlugin(Plugin):
    """AMFI Plugin for MFTools."""

    plugin_info = PluginInfo(
        name="AMFI",
        description="AMFI plugin for MFTools",
        version=mftools.__version__,
        author="Yashovardhan Dhanania",
        author_email="",
    )

    def __init__(self):
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
    """AMFI Source for MFTools."""

    LATEST_URL = "http://amfiindia.com/spages/NAVAll.txt"
    HISTORICAL_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={frm_dt}&todt={to_dt}"

    def __init__(self):
        """Initialize the AMFI source."""
        super().__init__()
        # Initialize any necessary attributes here.

    def _download_file(self, url, raw_file_path):
        """Download a file from the given URL."""
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an error for bad responses
            if "text/plain" not in response.headers["Content-Type"]:
                return False
            with open(raw_file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            logger.info(f"Downloaded historical data to {raw_file_path}")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to download historical data: {e}")
            return False
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return False

    def download_historical_data(
        self, raw_file_path, start_date, end_date, tickers=None
    ):
        """Download historical data from AMFI."""
        # Format the URLs with the provided dates.
        frm_dt = start_date.strftime("%d-%b-%Y")
        to_dt = end_date.strftime("%d-%b-%Y")
        url = self.HISTORICAL_URL.format(frm_dt=frm_dt, to_dt=to_dt)
        logger.debug(f"Downloading historical data from {url}")
        return self._download_file(url, raw_file_path)

    def download_latest_data(self, raw_file_path, tickers=None):
        """Download latest data from AMFI."""
        logger.debug(f"Downloading latest data from {self.LATEST_URL}")
        return self._download_file(self.LATEST_URL, raw_file_path)

    def process_data(self, raw_file_path):
        """Process the downloaded data."""
        # Implement the logic to process the downloaded data.
        pass

    def get_tickers(self):
        """Get the list of tickers."""
        with tempfile.TemporaryDirectory() as d:
            file_path = Path(d, "latest.txt")
            if self.download_latest_data(file_path):
                df = pl.scan_csv(
                    file_path,
                    separator=";",
                    null_values=["N.A.", "-"],
                    infer_schema=False,
                )
                df = df.drop_nulls(subset=["Date"])
                return df.select(
                    pl.col("Scheme Code").alias("symbol"),
                    pl.col("Scheme Name").alias("name"),
                    pl.coalesce(pl.col("^ISIN .*$")).alias("isin"),
                ).collect()
            else:
                return list()

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
            ticker_refresh_interval=timedelta(days=7),
            data_refresh_interval=timedelta(days=1),
        )


def register_plugin() -> AMFIPlugin:
    """Register the plugin with MFTools."""
    return AMFIPlugin()
