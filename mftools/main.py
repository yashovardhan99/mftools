"""Mutual Fund utilities for Python."""

from collections import defaultdict
from datetime import date, datetime
import logging
import importlib
import pkgutil

from mftools.models.helpers import ReturnFormat
from mftools.models.sources import Source, SourceInfo, Ticker
from mftools.plugins import __path__ as __plugins_path
from mftools.models.plugins import Plugin
from mftools.tickers import load_tickers, save_tickers
from mftools.utils import format_output, apply_filters, handle_input

from typing import Dict, Iterable, List, Literal, Optional, Tuple, Union, overload
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
                __plugins_path, "mftools.plugins."
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
        """Get the list of sources from all plugins.

        Args:
            source_keys (Optional[List[str]]): The source keys to filter by.
                Defaults to None.
                The source key is the key used to identify the source of the tickers.
                If source_keys is specified, only the sources from the specified keys
                are returned.
                For example, to filter by source key, you can use:
                source_key = ["amfi"]
                This will return only the sources from the AMFI source.
                If source_key is None, all sources from all plugins are returned.

        Returns:
            Iterable[SourceInfo]: An iterable of SourceInfo objects.
        """
        sources = self._get_sources(source_keys)
        return map(lambda source: source.get_source_info(), sources)

    @overload
    def get_tickers(
        self,
        filters: Optional[Dict[str, List[str]]] = None,
        source_keys: Optional[List[str]] = None,
        format: Literal[ReturnFormat.DICT] = ...,
    ) -> Dict[str, List]: ...

    @overload
    def get_tickers(
        self,
        filters: Optional[Dict[str, List[str]]] = None,
        source_keys: Optional[List[str]] = None,
        format: Literal[ReturnFormat.PL_DATAFRAME] = ...,
    ) -> pl.DataFrame: ...

    def get_tickers(
        self,
        filters: Optional[Dict[str, List[str]]] = None,
        source_keys: Optional[List[str]] = None,
        format: Union[ReturnFormat, str] = ReturnFormat.DICT,
    ) -> Union[Dict[str, List], pl.DataFrame, pl.LazyFrame, pd.DataFrame]:
        """Get the list of tickers from all plugins.

        Args:
            filters (Optional[Dict[str, List[str]]]): Filters to apply to the tickers.
                Defaults to None.
                See [mftools.utils.filter_tickers] for available filters.

            source_keys (Optional[List[str]]): The source keys to filter by.
                Defaults to None. If source_keys is specified, only the tickers
                from the specified sources are returned.
                If source_key is None, all tickers from all sources are returned.

            format (ReturnFormat): The format of the returned tickers.
                Defaults to ReturnFormat.DICT. See [mftools.models.helpers.ReturnFormat] for available formats.

        Returns:
            Union[Iterable[Ticker], pl.DataFrame, pd.DataFrame]: The list of tickers in the specified format.
        """
        df_tickers = Ticker.polars_schema.to_frame(eager=False).with_columns(
            pl.lit(None).cast(pl.String()).alias("source_key"),
        )
        pre_loaded_sources = defaultdict(lambda: datetime.min)
        try:
            logger.debug("Getting locally saved tickers")
            df_tickers_pre = load_tickers()
            logger.debug("Loaded locally saved tickers")
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
            # This will run for all sources matching the source_keys (if applicable)
            key = source.get_source_key()
            if (
                key in pre_loaded_sources
                and (datetime.now() - pre_loaded_sources[key])
                < source.get_source_info().ticker_refresh_interval
            ):
                logger.debug(f"Skipping source {key} as it is up to date")
            else:
                logger.debug(f"Getting tickers from source {key}")
                all_tickers.append(
                    handle_input(
                        source.get_tickers(), Ticker.polars_schema
                    ).with_columns(pl.lit(key).alias("source_key"))
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

        logger.debug("Combining tickers with locally saved tickers")
        df_tickers = df_tickers_pre.update(
            df_tickers,
            on=["source_key", "symbol"],
            how="full",
        )

        logger.debug("Saving all tickers to local file")
        save_tickers(df_tickers)

        logger.debug("Applying filters to tickers")
        df_tickers = apply_filters(df_tickers, source_keys, filters)

        return format_output(df_tickers, format)

    def get_quotes(
        self,
        *tickers: Union[str, Tuple[str, str]],
        start_date: date = date.today(),
        end_date: date = date.today(),
        format: Union[ReturnFormat, str] = ReturnFormat.DICT,
    ) -> Union[Iterable[pl.DataFrame], pl.DataFrame, pl.LazyFrame, pd.DataFrame]:
        """Get the quotes for the specified tickers.

        Args:
            tickers (Union[str, Tuple[str, str]]): The tickers to get quotes for.
                This can be a single symbol, a list of symbols, or a list of tuples
                containing the symbol and the source key.
                If source key is not specified, all available sources are checked.

            start_date (date): The start date for the quotes. Defaults to today.

            end_date (date): The end date for the quotes. Defaults to today.

            format (ReturnFormat): The format of the returned tickers.
                Defaults to ReturnFormat.DICT. See [mftools.models.helpers.ReturnFormat] for available formats.

        Returns:
            Union[Iterable[pl.DataFrame], pl.DataFrame, pd.DataFrame]: The quotes in the specified format.

        Example:
            >>> mftools = MFTools()
            >>> quotes = mftools.get_quotes("500209", "500210", ("500211", "amfi"))
        """
        pass
