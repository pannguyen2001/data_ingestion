import json

from loguru import logger


def read_checkpoint(file_path: str = "checkpoint/checkpoint.json") -> dict | None:
    """Read checkpoint data."""

    with open(file_path, "r") as f:
        try:
            checkpoint = json.load(f)
            return checkpoint if checkpoint else {}

        except json.decoder.JSONDecodeError as e:
            logger.error(e)
            return None
