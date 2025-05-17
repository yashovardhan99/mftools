"""This module contains tests for the `amfi` plugin.

Classes:
    MockTextResponse: A mock response object simulating a text/plain HTTP response for testing purposes.

Fixtures:
    mock_requests: Pytest fixture that mocks the `requests.get` method to return a mock response for specific URLs.

Test Functions:
    test_file_download_success: Tests that `download_file` successfully downloads and writes content to a file for a valid URL.
    test_file_download_invalid_url: Tests that `download_file` handles invalid URLs gracefully and logs an error.
"""

from pathlib import Path
import pytest
import requests

from mftools.plugins import amfi
from mftools.models.plugins import PluginInfo, Plugin
import datetime


class MockTextResponse:
    """A mock response class simulating a text-based HTTP response object."""

    @staticmethod
    def raise_for_status():
        """Mock method to simulate no error on status check."""
        return None

    headers = {"Content-Type": "text/plain"}

    @staticmethod
    def iter_content(chunk_size=8192):
        """Mock method to simulate streaming content."""
        yield b"Test Header"
        yield b"Test Data"


@pytest.fixture
def mock_requests_success(monkeypatch: pytest.MonkeyPatch):
    """Mock the requests.get method to return a mock response."""

    def mock_get(url: str, *args, **kwargs):
        return MockTextResponse()

    monkeypatch.setattr(requests, "get", mock_get)


def test_download_historical_data_success(mock_requests_success: None, tmp_path: Path):
    """Test the download_file function with a valid URL."""
    # Create a temporary directory
    temp_dir = tmp_path / "temp_dir"
    temp_dir.mkdir(parents=True, exist_ok=True)
    filename = "test.txt"
    f = temp_dir / filename

    # Call the download_file function
    result = (
        amfi.register_plugin()
        .get_sources()[0]
        .download_historical_data(
            f, datetime.date(2025, 1, 1), datetime.date(2025, 1, 31)
        )
    )

    # Check if the file was downloaded successfully
    assert result
    assert f.exists()
    assert f.is_file()
    assert f.stat().st_size > 0
    with open(f, "rb") as file:
        content = file.read()
        assert b"Test Header" in content
        assert b"Test Data" in content


def test_download_latest_data_success(mock_requests_success: None, tmp_path: Path):
    """Test the download_file function with a valid URL."""
    # Create a temporary directory
    temp_dir = tmp_path / "temp_dir"
    temp_dir.mkdir(parents=True, exist_ok=True)
    filename = "test.txt"
    f = temp_dir / filename

    # Call the download_file function
    result = amfi.register_plugin().get_sources()[0].download_latest_data(f)

    # Check if the file was downloaded successfully
    assert result
    assert f.exists()
    assert f.is_file()
    assert f.stat().st_size > 0
    with open(f, "rb") as file:
        content = file.read()
        assert b"Test Header" in content
        assert b"Test Data" in content


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
