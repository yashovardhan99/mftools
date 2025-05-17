"""Helper models for MFTools."""

from enum import Enum


class ReturnFormat(Enum):
    """Enum for return formats."""

    LIST = "list"
    PL_DATAFRAME = "pl_dataframe"
    PL_LAZYFRAME = "pl_lazyframe"
    PD_DATAFRAME = "pd_dataframe"
