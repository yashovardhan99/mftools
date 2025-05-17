"""Define a common interface for all plugins."""

import abc
from collections.abc import Iterable

from mftools.models.sources import Source


class PluginInfo:
    """Class to hold plugin information."""

    def __init__(self, name, description, version, author, author_email):
        """Initialize the plugin info."""
        self.name = name
        self.description = description
        self.version = version
        self.author = author
        self.author_email = author_email

    def __repr__(self):
        """Return a string representation of the plugin info."""
        return f'PluginInfo(name="{self.name}", description="{self.description}", version="{self.version}", author="{self.author}", author_email="{self.author_email}")'


class Plugin(abc.ABC):
    """Base class for all plugins."""

    def __init__(self) -> None:
        """Initialize the plugin."""
        super().__init__()

    @classmethod
    @abc.abstractmethod
    def get_info(cls) -> PluginInfo:
        """Return plugin information."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abc.abstractmethod
    def get_sources(self) -> Iterable[Source]:
        """Return a list of sources for the plugin."""
        raise NotImplementedError("Subclasses must implement this method.")
