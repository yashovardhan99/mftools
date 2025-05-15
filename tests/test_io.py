import stat

import pytest
import requests

from mftools.io.download import download_file


class MockTextResponse:
    @staticmethod
    def raise_for_status():
        return None

    headers = {"Content-Type": "text/plain"}

    @staticmethod
    def iter_content(chunk_size=8192):
        yield b"Test Header"
        yield b"Test Data"

    @staticmethod
    def close():
        pass


@pytest.fixture
def mock_requests(monkeypatch: pytest.MonkeyPatch):
    """Mock the requests.get method to return a mock response."""

    def mock_get(url: str, *args, **kwargs):
        requests.Response()
        if url.startswith(
            "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
        ):
            return MockTextResponse()
        else:
            raise requests.HTTPError("Invalid URL")

    monkeypatch.setattr(requests, "get", mock_get)


def test_file_download_success(mock_requests, tmp_path):
    """Test the download_file function with a valid URL."""
    # Create a temporary directory
    temp_dir = tmp_path / "temp_dir"
    temp_dir.mkdir(parents=True, exist_ok=True)
    url = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?tp=1&frmdt=01-Oct-2018&todt=01-Oct-2018"
    filename = "test.txt"
    f = temp_dir / filename

    # Call the download_file function
    result = download_file(url, str(temp_dir), filename)

    # Check if the file was downloaded successfully
    assert result == str(f)
    assert f.exists()
    assert f.is_file()
    assert f.stat().st_size > 0
    with open(f, "rb") as file:
        content = file.read()
        assert b"Test Header" in content
        assert b"Test Data" in content


def test_file_download_invalid_url(mock_requests, tmp_path, caplog):
    """Test the download_file function with an invalid URL."""
    # Create a temporary directory
    temp_dir = tmp_path / "temp_dir"
    temp_dir.mkdir(parents=True, exist_ok=True)
    url = "https://invalid.url"
    filename = "test.txt"

    # Call the download_file function
    result = download_file(url, str(temp_dir), filename)

    # Check if the file was not downloaded
    assert result is None

    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "ERROR"
    assert "HTTPError" in caplog.text
