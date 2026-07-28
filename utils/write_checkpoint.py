import json

from loguru import logger


def write_checkpoint(checkpoint: dict, file_path: str = "checkpoint/checkpoint.json") -> None:
    """Save checkpoint data."""

    try:
        if checkpoint is None:
            raise ValueError("checkpoint is None.")
        if not checkpoint:
            raise ValueError("checkpoint is empty.")

        with open(file_path, "w") as f:
            json.dump(checkpoint, f, indent=4)

    except Exception as e:
        logger.error(e)
        raise