"""Utility functions for mftools."""

from pathlib import Path
import platformdirs


def get_tickers_dir() -> Path:
    """Get the directory for tickers."""
    return Path(platformdirs.user_data_dir("mftools")).joinpath("tickers")
