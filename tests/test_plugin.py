"""Test plugin related functionality."""

import pytest
import niveshpy
from niveshpy.models.plugins import Plugin, PluginInfo


class DummyPlugin(Plugin):
    """Test plugin for NiveshPy."""

    def __init__(self):
        """Initialize the Test plugin."""
        super().__init__()

    def get_sources(self):
        """Return a list of sources for the plugin."""
        return []

    @classmethod
    def get_info(cls):
        """Return plugin information."""
        return PluginInfo(
            name="TestPlugin",
            description="A test plugin for NiveshPy",
            version=niveshpy.__version__,
            author="Test Author",
            author_email="test@example.com",
        )


def test_register_plugin_success():
    """Test the registration of the AMFI plugin."""
    app = niveshpy.Nivesh()
    plugin = DummyPlugin()
    app.register_plugin(plugin)

    assert plugin in app.plugins


def test_register_plugin_failure():
    """Test the registration of the AMFI plugin."""
    app = niveshpy.Nivesh()
    plugin = "NotAPlugin"
    with pytest.raises(TypeError):
        app.register_plugin(plugin)
