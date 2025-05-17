"""Mutual Fund utilities for Python."""

from collections import defaultdict
from datetime import datetime
import logging
import importlib
import pkgutil
import warnings

from mftools.models.helpers import ReturnFormat
from mftools.models.sources import Source, SourceInfo, Ticker
import mftools.plugins
from mftools.models.plugins import Plugin
import mftools.tickers as _tickers

from typing import Dict, Iterable, List, Optional, Union
import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)


def _import_plugin(name: str) -> Optional[Plugin]:
    """Import a plugin by name."""
    try:
        module = importlib.import_module(name)
        if hasattr(module, "register_plugin"):
            plugin = module.register_plugin()
            if isinstance(plugin, Plugin):
                return plugin
            else:
                logger.warning(f"Plugin {name} does not inherit from Plugin class")
        else:
            logger.warning(f"Plugin {name} does not have register_plugin() method")
    except ImportError:
        logger.exception(f"Failed to import plugin {name}")
    return None


def _import_local_plugins() -> Iterable[Plugin]:
    """Import all local plugins."""
    return filter(
        None,
        (
            _import_plugin(name)
            for _, name, ispkg in pkgutil.iter_modules(
                mftools.plugins.__path__, "mftools.plugins."
            )
            if not ispkg
        ),
    )


class MFTools:
    """This is the base class for MFTools."""

    def __init__(self):
        """Initialize the MFTools class."""
        logger.debug("Initializing MFTools")
        local_plugins = _import_local_plugins()
        self.plugins: List[Plugin] = []
        for plugin in local_plugins:
            logger.debug(f"Loaded plugin: {plugin.get_info()}")
            self.plugins.append(plugin)
        logger.debug(f"Loaded {len(self.plugins)} plugins")
        logger.debug("MFTools initialized")

    def register_plugin(self, plugin: Plugin):
        """Register a custom plugin."""
        if isinstance(plugin, Plugin):
            self.plugins.append(plugin)
            logger.info(f"Registered plugin: {plugin.get_info()}")
        else:
            raise TypeError("Plugin must be an instance of Plugin class")

    def _get_sources(self, source_keys: Optional[List[str]] = None) -> List[Source]:
        """Get the list of sources from all plugins."""
        logger.debug("Getting sources from all plugins")
        sources = []
        for plugin in self.plugins:
            for source in plugin.get_sources():
                if source_keys is None or source.get_source_key() in source_keys:
                    sources.append(source)
                    logger.debug(f"Loaded source: {source.get_source_info()}")
        logger.debug(f"Loaded {len(sources)} sources")
        return sources

    def get_sources(
        self, source_keys: Optional[List[str]] = None
    ) -> Iterable[SourceInfo]:
        """Get the list of sources from all plugins."""
        sources = self._get_sources(source_keys)
        return map(lambda source: source.get_source_info(), sources)

    def get_tickers(
        self,
        filters: Optional[Dict[str, List[str]]] = None,
        source_keys: Optional[List[str]] = None,
        format: Union[ReturnFormat, str] = ReturnFormat.LIST,
    ) -> Union[Iterable[Ticker], pl.DataFrame, pl.LazyFrame, pd.DataFrame]:
        """Get the list of tickers from all plugins.

        Args:
            format (ReturnFormat): The format of the returned tickers.
                Defaults to ReturnFormat.LIST.
                The available formats are:
                - ReturnFormat.LIST ("list"): Returns a list of Ticker objects.
                - ReturnFormat.PL_DATAFRAME ("pl_dataframe"): Returns a Polars DataFrame.
                - ReturnFormat.PL_LAZYFRAME ("pl_lazyframe"): Returns a Polars LazyFrame.
                - ReturnFormat.PD_DATAFRAME ("pd_dataframe"): Returns a Pandas DataFrame.

            filters (Optional[Dict[str, List[str]]]): Filters to apply to the tickers.
                Defaults to None.
                The filters are applied to the tickers returned by the plugins.
                All filters and combined using OR. The keys of the dictionary are
                the column names and the values are lists of values to filter by.
                For example, to filter by symbol or name, you can use:
                filters = {
                    "symbol": ["0500209", "500210"],
                    "name": ["UTI Nifty Next 50 Index Fund - Direct"]
                }
                This will return only the tickers that match the specified symbol or name.
                If filters is None, all tickers are returned.

            source_keys (Optional[List[str]]): The source keys to filter by.
                Defaults to None.
                The source key is the key used to identify the source of the tickers.
                If source_keys is specified, only the tickers from the specified sources
                are returned.
                For example, to filter by source key, you can use:
                source_key = ["amfi"]
                This will return only the tickers from the AMFI source.
                If source_key is None, all tickers from all sources are returned.

        Returns:
            Union[Iterable[Ticker], pl.DataFrame, pd.DataFrame]: The list of tickers in the specified format.
        """
        logger.debug("Getting locally saved tickers")
        df_tickers = pl.LazyFrame(schema=Ticker.polars_schema).with_columns(
            pl.lit(None).cast(pl.String()).alias("source_key"),
        )
        pre_loaded_sources = defaultdict(lambda: datetime.min)
        try:
            df_tickers_pre = _tickers.load_tickers()
            logger.debug("Loaded locally saved tickers")
            df_tickers_pre = _tickers.filter_tickers(
                df_tickers_pre, source_keys, filters
            )
            df_sources = (
                df_tickers_pre.group_by("source_key")
                .agg(pl.min("last_updated").alias("last_updated"))
                .collect()
            )
            for row in df_sources.iter_rows(named=True):
                source_key = row["source_key"]
                last_updated = row["last_updated"]
                pre_loaded_sources[source_key] = last_updated
        except FileNotFoundError:
            logger.debug("No locally saved tickers found")
            df_tickers_pre = df_tickers.clone().with_columns(
                pl.lit(None).cast(pl.Datetime()).alias("last_updated"),
            )

        logger.debug("Getting tickers from all plugins")
        all_tickers = [df_tickers]
        for source in self._get_sources(source_keys):
            key = source.get_source_key()
            if source_keys and key not in source_keys:
                logger.debug(
                    f"Skipping source {source.get_source_info()} as it is not in the specified source keys"
                )
                continue
            if (
                key in pre_loaded_sources
                and (datetime.now() - pre_loaded_sources[key])
                < source.get_source_info().ticker_refresh_interval
            ):
                logger.debug(f"Skipping source {key} as it is up to date")
                continue

            tickers = source.get_tickers()
            if isinstance(tickers, (pl.DataFrame, pl.LazyFrame)):
                all_tickers.append(
                    tickers.with_columns(pl.lit(key).alias("source_key")).lazy()
                )
            elif isinstance(tickers, pd.DataFrame):
                all_tickers.append(
                    pl.from_pandas(tickers, schema_overrides=Ticker.polars_schema)
                    .with_columns(pl.lit(key).alias("source_key"))
                    .lazy()
                )
            elif isinstance(tickers, Iterable):
                all_tickers.append(
                    pl.from_dicts(
                        map(Ticker.to_dict, tickers), schema=Ticker.polars_schema
                    )
                    .with_columns(pl.lit(key).alias("source_key"))
                    .lazy()
                )
            else:
                warnings.warn(
                    f'Invalid type {type(tickers)} received for source_key = "{source.get_source_key()}". Please contact the plugin author.'
                )

        logger.debug(f"Loaded {len(all_tickers)} tickers from all plugins")

        logger.debug("Combining tickers from all plugins")
        df_tickers = pl.concat(all_tickers, how="vertical_relaxed").select(
            pl.col("symbol").cast(pl.String()),
            pl.col("name").cast(pl.String()),
            pl.col("isin").cast(pl.String()),
            pl.col("source_key").cast(pl.String()),
            pl.lit(datetime.now()).alias("last_updated"),
        )

        logger.debug("Applying filters to tickers")
        df_tickers = _tickers.filter_tickers(df_tickers, source_keys, filters)

        logger.debug("Combining tickers with locally saved tickers")
        df_tickers = df_tickers_pre.update(
            df_tickers,
            on=["source_key", "symbol"],
            how="full",
        )

        logger.debug("Saving tickers to local file")
        _tickers.save_tickers(df_tickers)

        if isinstance(format, str):
            format = ReturnFormat(format)
        if format == ReturnFormat.LIST:
            return map(
                Ticker.from_dict, df_tickers.drop("source_key").collect().to_dicts()
            )
        elif format == ReturnFormat.PL_DATAFRAME:
            return df_tickers.collect()
        elif format == ReturnFormat.PL_LAZYFRAME:
            return df_tickers
        elif format == ReturnFormat.PD_DATAFRAME:
            return df_tickers.collect().to_pandas()
        else:
            raise ValueError("Invalid format specified")
