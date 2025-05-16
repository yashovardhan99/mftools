"""Mutual Fund utilities for Python."""

import logging
import importlib
import pkgutil
import warnings

from mftools.models.helpers import ReturnFormat
from mftools.models.sources import Ticker
import mftools.plugins
from mftools.models.plugins import Plugin

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

    def get_tickers(
        self,
        filters: Optional[Dict[str, List[str]]] = None,
        source_key: Optional[List[str]] = None,
        format: Union[ReturnFormat, str] = ReturnFormat.LIST,
    ) -> Union[Iterable[Ticker], pl.DataFrame, pd.DataFrame]:
        """Get the list of tickers from all plugins.

        Args:
            format (ReturnFormat): The format of the returned tickers.
                Defaults to ReturnFormat.LIST.
                The available formats are:
                - ReturnFormat.LIST ("list"): Returns a list of Ticker objects.
                - ReturnFormat.PL_DATAFRAME ("pl_dataframe"): Returns a Polars DataFrame.
                - ReturnFormat.PD_DATAFRAME ("pd_dataframe"): Returns a Pandas DataFrame.

            filters (Optional[Dict[str, List[str]]]): Filters to apply to the tickers.
                Defaults to None.
                The filters are applied to the tickers returned by the plugins.
                The keys of the dictionary are the column names and the values are
                lists of values to filter by.
                For example, to filter by symbol or name, you can use:
                filters = {
                    "symbol": ["0500209", "500210"],
                    "name": ["UTI Nifty Next 50 Index Fund - Direct"]
                }
                This will return only the tickers that match the specified symbol or name.
                If filters is None, all tickers are returned.

            source_key (Optional[List[str]]): The source key to filter by.
                Defaults to None.
                The source key is the key used to identify the source of the tickers.
                If source_key is specified, only the tickers from the specified sources
                are returned.
                For example, to filter by source key, you can use:
                source_key = ["amfi"]
                This will return only the tickers from the AMFI source.
                If source_key is None, all tickers from all sources are returned.

        Returns:
            Union[Iterable[Ticker], pl.DataFrame, pd.DataFrame]: The list of tickers in the specified format.
        """
        logger.debug("Getting tickers from all plugins")
        df_tickers = pl.DataFrame(schema=Ticker.polars_schema)
        all_tickers = [df_tickers]
        for plugin in self.plugins:
            for source in plugin.get_sources():
                if source_key is None or source.get_source_key() in source_key:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"Getting tickers from plugin: {plugin.get_info()}"
                        )

                    tickers = source.get_tickers()
                    if isinstance(tickers, pl.DataFrame):
                        all_tickers.append(tickers)
                    elif isinstance(tickers, pd.DataFrame):
                        all_tickers.append(pl.from_pandas(tickers))
                    elif isinstance(tickers, Iterable):
                        all_tickers.append(
                            pl.DataFrame(
                                [ticker.to_dict() for ticker in tickers]
                            ).with_columns(
                                pl.lit(source.get_source_key()).alias("source_key")
                            )
                        )
                    else:
                        warnings.warn(
                            f'Plugin {plugin.get_info().name} has returned an invalid type {type(tickers)} for source_key = "{source.get_source_key()}"'
                        )

        logger.debug("Combining tickers from all plugins")
        df_tickers = pl.concat(all_tickers, how="vertical_relaxed").select(
            pl.col("symbol").cast(pl.String()),
            pl.col("name").cast(pl.String()),
            pl.col("isin").cast(pl.String()),
            pl.col("source_key").cast(pl.String()),
        )

        if filters:
            logger.debug("Applying filters to tickers")
            expressions = None
            for column, values in filters.items():
                if column in df_tickers.columns:
                    if expressions is None:
                        expressions = pl.lit(False)
                    expressions = expressions | pl.col(column).is_in(values)
                else:
                    warnings.warn(f"Filter column {column} not found in tickers")
            if expressions is not None:
                df_tickers = df_tickers.filter(expressions)

        if isinstance(format, str):
            format = ReturnFormat(format)
        if format == ReturnFormat.LIST:
            return map(Ticker.from_dict, df_tickers.drop("source_key").to_dicts())
        elif format == ReturnFormat.PL_DATAFRAME:
            return df_tickers
        elif format == ReturnFormat.PD_DATAFRAME:
            return df_tickers.to_pandas()
        else:
            raise ValueError("Invalid format specified")
