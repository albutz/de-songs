"""Helper functions for docker."""
import logging

# from time import sleep
from types import TracebackType
from typing import Optional, Type

from python_on_whales import DockerClient


class Docker:
    """Context manager for docker compose."""

    def __init__(self, client: DockerClient):
        """Init the docker client.

        Args:
            client: Docker client object
        """
        self.client = client

    def __enter__(self) -> None:
        """Start up Postgres container."""
        self.client.compose.up(detach=True)
        logging.info("Postgres container started.")

    def __exit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_val: Optional[Exception],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Stop Postgres container.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
        """
        self.client.compose.down()
        logging.info("Postgres container stopped.")
