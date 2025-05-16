"""Built-in plugins for MFTools."""

from .main import MFTools
from importlib.metadata import version

__all__ = ["MFTools"]

__version__ = version(__name__)
