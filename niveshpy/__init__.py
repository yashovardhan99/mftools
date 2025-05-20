"""Built-in plugins for NiveshPy."""

from .main import Nivesh
from importlib.metadata import version

__all__ = ["Nivesh"]

__version__ = version(__name__)
