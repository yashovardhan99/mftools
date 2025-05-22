"""This module contains tests for the `amfi` plugin.

Classes:
    MockTextResponse: A mock response object simulating a text/plain HTTP response for testing purposes.

Fixtures:
    mock_requests: Pytest fixture that mocks the `requests.get` method to return a mock response for specific URLs.

Test Functions:
    test_file_download_success: Tests that `download_file` successfully downloads and writes content to a file for a valid URL.
    test_file_download_invalid_url: Tests that `download_file` handles invalid URLs gracefully and logs an error.
"""

from niveshpy.plugins import amfi
from niveshpy.models.plugins import PluginInfo, Plugin


def test_register_plugin():
    """Test the register_plugin function."""
    plugin = amfi.register_plugin()
    assert plugin is not None
    assert isinstance(plugin, Plugin)


def test_get_plugin_info():
    """Test the get_plugin_info function."""
    plugin = amfi.register_plugin()
    plugin_info = plugin.get_info()
    assert plugin_info is not None
    assert isinstance(plugin_info, PluginInfo)
