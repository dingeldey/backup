"""
Collection of simple logger utility functions. As I do not want to place an ini file to the script, I
supply functions to create an and later delete it on completion of the code.
"""
import os
from os import PathLike
from pathlib import Path


def create_logger_ini(ini_filename: PathLike, log_filename: PathLike):
    """
    Creates a logger ini which is used to set up the project logger.
    @param log_filename: name of log file to be written
    @param ini_filename: name of ini name to be created
    """

    log_filename_rep = str(log_filename)
    if os.name == 'nt':
        log_filename_rep = str(log_filename).replace("\\", "/")

    logger_ini_content = """[formatters]
        keys=default
        [formatter_default]
        format=<%(levelname)-3s><%(asctime)s> %(message)s <%(filename)s:%(lineno)d>'
        class=logging.Formatter
        [handlers]
        keys=console, file
        [handler_console]
        class=logging.StreamHandler
        formatter=default
        args=tuple()
        [handler_file]
        class=logging.FileHandler
        level=INFO
        formatter=default
        args=("{}", "w")
        [loggers]
        keys=root
        [logger_root]
        level=INFO
        formatter=default
        handlers=console,file""".format(log_filename_rep)

    ini_exists: bool = os.path.isfile(ini_filename)
    if not ini_exists:
        with open(ini_filename, 'w') as f:
            f.write(logger_ini_content)


def remove_logger_ini(filename: PathLike):
    """
    Removes the previously written log file.
    @param filename: ini file name
    """
    ini_exists: bool = os.path.isfile(filename)
    if ini_exists:
        os.remove(filename)
