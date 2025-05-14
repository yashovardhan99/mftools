"""mftools download module.

This module provides a function to download files from a URL and save them to a specified directory.
"""

import logging
import pathlib
from typing import Optional

import requests

logger = logging.getLogger(__name__)


def download_file(url: str, directory: str, filename: str) -> Optional[str]:
    """Download a text file from a URL and save it to a specified directory.

    Args:
        url (str): The URL of the file to download.
        directory (str): The directory where the file will be saved. This can be a temporary directory.
        filename (str): The name to save the file as.

    Returns:
        Optional[str]: The path to the downloaded file if successful, None otherwise.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad responses
        if "text/plain" not in response.headers["Content-Type"]:
            logger.warning(f"Skipping non-text file: {filename}")
            return None
        path = pathlib.Path(directory, filename)
        with path.open("wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        return file.name
    except requests.HTTPError:
        logger.exception("HTTP error occurred")
        return None
    except requests.ConnectionError:
        logger.exception("Connection error occurred")
        return None
    except requests.Timeout:
        logger.exception("Timeout error occurred")
        return None
    except requests.TooManyRedirects:
        logger.exception("Too many redirects")
        return None
    except requests.RequestException:
        logger.exception(f"Error downloading {filename}")
        return None
    except BlockingIOError:
        logger.exception("BlockingIOError occurred")
        return None
