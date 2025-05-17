"""Helper models for MFTools."""

from enum import Enum


class ReturnFormat(Enum):
    """Enum for return formats.

    This enum defines the different formats in which data can be returned.

    Attributes:
        DICT: Format as a dictionary mapping column names as keys.
        PL_DATAFRAME: Format as a Polars DataFrame.
        PL_LAZYFRAME: Format as a Polars LazyFrame. This is the internal format used by mftools.
        PD_DATAFRAME: Format as a Pandas DataFrame.
        JSON: Format as a JSON string.
        CSV: Format as a CSV string.
    """

    DICT = "dict"
    PL_DATAFRAME = "pl_dataframe"
    PL_LAZYFRAME = "pl_lazyframe"
    PD_DATAFRAME = "pd_dataframe"
    JSON = "json"
    CSV = "csv"
