"""
Collection of simple logger utility functions. As I do not want to place an ini file to the script, I
supply functions to create an and later delete it on completion of the code.
"""
import os
from os import PathLike
from pathlib import Path
from submodules.python_core_libs.logging.project_logger import Log


def set_up_logger(log_destination: str, timestamp: str):
    """
    Set up logger. Formatting etc.
    """
    logfile_path: PathLike[str] = Path(os.path.join(log_destination, Path(timestamp + '.log')))
    if os.path.isfile(logfile_path):
        raise Exception("You are triggering to program to quickly, wait at least a second as the timestamps only have a one second resolution")

    logger = Log.instance().set_up_logger(logfile_path).logger
    logger.info(f"Writing log to {logfile_path}.")
