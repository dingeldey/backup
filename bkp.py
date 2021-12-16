import argparse
import sys
import os.path
import datetime
import traceback
import configparser
from submodules.python_core_libs.logging.project_logger import Log


def create_logger_ini(ini_filename: str, log_filename: str):
    """
    Creates a logger ini which is used to setup the project logger.
    @param log_filename: name of log file to be written
    @param ini_filename: name of ini name to be created
    """
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
        handlers=console,file""".format(log_filename)

    ini_exists: bool = os.path.isfile(ini_filename)
    if not ini_exists:
        with open(ini_filename, 'w') as f:
            f.write(logger_ini_content)


def remove_logger_ini(filename: str):
    """
    Removes the previously written log file.
    @param filename: ini file name
    """
    ini_exists: bool = os.path.isfile(filename)
    if ini_exists:
        os.remove(filename)


def create_backup_base_folder(timestamp: str, destination_path: str):
    """
    Creates the folder for the current backup.
    @param timestamp: time stamp of the current run.
    @param destination_path: Path of backup root folder
    """
    logger = Log.instance().logger
    backup_root_exists = os.path.isdir(destination_path)
    if not backup_root_exists:
        os.mkdir(destination_path)

    # then create the folder for the current run.
    current_full_path_base = os.path.join(destination_path, "0")
    current_full_exists = os.path.isdir(current_full_path_base)
    if current_full_exists:
        # name of previous full backup after the new will be created.
        # for this we read the time stamp of the current full backup in order to be able to rename it properly.
        config = configparser.ConfigParser()
        config.read(os.path.join(current_full_path_base, 'cfg.ini'))
        rename_path = os.path.join(destination_path, str(config['DEFAULT']['timestamp']))
        logger.info(f"Moving full backup from current {current_full_path_base} to {rename_path}")
        os.rename(current_full_path_base, rename_path)

    # after having moved a possibly existing full backup we now create a new one
    os.mkdir(current_full_path_base)
    config = configparser.ConfigParser()
    config['DEFAULT']['timestamp'] = timestamp

    with open(os.path.join(current_full_path_base, 'cfg.ini'), 'w') as configfile:  # save
        config.write(configfile)


def backup(timestamp: str):
    """
    @param timestamp: timestamp to identify backup
    """
    logger = Log.instance().logger
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--incremental', action='store_true', help="Indicate an incremental backup is desired.")
    parser.add_argument('-d', '--destination', help="Path to destination")
    parser.add_argument('-s', '--sources', nargs='+', default=[], help="List of sources, comma separated.")
    args, unknown = parser.parse_known_args()

    if not args.destination:
        raise Exception("No destination via the -d flag specified. See --help.")

    if not args.sources:
        raise Exception("No sources via the -s flag specified. See --help.")

    create_backup_base_folder(timestamp, args.destination)

    #cp -al data1/ d


def main():
    remove_logger_ini("logs/logger.ini")
    logger = Log.instance().logger
    timestamp = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()).replace(" ", "_")
    # timestamp = ('%s' % datetime.datetime.now()).replace(" ", "--").replace(":", "-")
    print(timestamp + '.log')
    create_logger_ini("logs/logger.ini", "logs/" + timestamp + '.log')
    Log.instance().set_ini("logs/logger.ini")
    try:
        backup(timestamp)
    except Exception as e:
        logger.error(e)
        logger.error('\n' + traceback.format_exc())
    finally:
        remove_logger_ini("logs/logger.ini")


if __name__ == '__main__':
    main()
