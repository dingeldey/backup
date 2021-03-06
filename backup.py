import argparse
import configparser
import json
import logging
import os.path
import shutil
import sys
import traceback
from typing import List, Tuple
from submodules.python_core_libs.logging.project_logger import Log
from utils.changesummary import ChangeSummary
from utils.datetimeutils import *
from utils.log_zipper import LogZipper
from utils.loggerutils import set_up_logger
from utils.rsync_caller import RsyncCaller
from utils.rsyncpolicy import RsyncPolicy
from pathlib import Path
from os import PathLike


def get_current_series_name():
    """
    Return name of current backup series
    @return: name of current backup series
    """
    return "active_series"


def get_path_to_backup_series(destination_path: PathLike) -> PathLike:
    """
    @param destination_path: Backup path
    @return: Returns the backup-path with a simple addition to indicate where the current backup-series is being placed.
    """
    return Path(os.path.join(destination_path, Path(get_current_series_name())))


def get_active_backup_path(timestamp: str, destination_path: PathLike, incremental: bool, continuing: bool) \
        -> PathLike:
    """
    Create a folder to backup to as well as moving old backups

    @param timestamp: time stamp of the current run.
    @param destination_path: Path of backup root folder
    @param incremental: Indicates if an incremental backup is wanted.
    @param continuing: Indicates that a previously failed backup is continued.
    @return: Returns the active backup path
    """
    logger = Log.instance().logger
    if not os.path.isdir(destination_path):
        os.mkdir(destination_path)

    # then create the folder for the current run.
    path_to_backup_series: PathLike[str] = get_path_to_backup_series(destination_path)
    if not os.path.isdir(path_to_backup_series) and incremental:
        logger.warning(f"No full backup to build on exists - should be found here {path_to_backup_series}."
                       f" Running a full backup first.")
        incremental = False

    if incremental:
        return incremental_backup(path_to_backup_series, timestamp, continuing)
    else:
        return full_backup(destination_path, timestamp)


def full_backup(destination_path: PathLike, timestamp: str) -> PathLike:
    """
    Delegates the full backup run.
    @param destination_path:
    @param timestamp: time stamp of backup run
    @return:
    """
    logger = Log.instance().logger
    logger.info("Starting full backup.")
    path_to_backup_series: PathLike = get_path_to_backup_series(destination_path)
    current_full_exists: bool = os.path.isdir(path_to_backup_series)
    if current_full_exists:
        move_previous_backup(path_to_backup_series, destination_path)

    active_path: PathLike = make_folder_for_new_full_backup(path_to_backup_series, timestamp)
    return active_path


def make_folder_for_new_full_backup(path_to_backup_series: PathLike, timestamp: str) -> PathLike:
    """
    Create a folder for the current backup run.
    @param path_to_backup_series: Base path to current backup series.
    @param timestamp: Timestamp of current backup run
    @return: Returns the path where to place the backup.
    """
    logger = Log.instance().logger
    # after having moved a possibly existing full backup we now create a new one
    if not os.path.isdir(path_to_backup_series):
        os.mkdir(path_to_backup_series)
    # lastly we have to create the currently active backup folder.
    active_path: PathLike[str] = Path(os.path.join(path_to_backup_series, Path(timestamp)))
    if not os.path.isdir(active_path):
        logger.info(f"Creating folder {active_path} for this backup run.")
        os.mkdir(active_path)
    else:
        logger.warning(f"Folder {active_path} already exists. This is probably filling run.")
    return active_path


def move_previous_backup(path_to_backup_series: PathLike, destination_path: PathLike):
    """
    Moves the last backup series to a new path which is named by its most recent update within the series.
    @param path_to_backup_series: Path to backup series.
    @param destination_path: path to where the backup series shall be written
    """
    logger = Log.instance().logger
    # name of previous full backup after the new will be created.
    # for this we read the time stamp of the current full backup in order to be able to rename it properly.
    config = configparser.ConfigParser()
    config.read(os.path.join(path_to_backup_series, 'cfg.ini'))
    with open(os.path.join(path_to_backup_series, 'cfg.ini'), 'w') as configfile:  # save
        config.write(configfile)
    timestamp_of_last_backup: str = get_timestamp_of_last_backup(config)
    if not timestamp_of_last_backup:
        return
    new_path_of_previous_backup_series = os.path.join(destination_path, timestamp_of_last_backup)
    logger.info(f"Moving full backup from current {path_to_backup_series} to {new_path_of_previous_backup_series}")
    os.rename(path_to_backup_series, new_path_of_previous_backup_series)


def incremental_backup(path_to_backup_series: PathLike, timestamp: str, continuing: bool) -> PathLike:
    """
    Creates or returns (when continuing) active folder for incremental backup.
    @param path_to_backup_series: Backup series folder where all incrementals are saved.
    @param timestamp: timestamp of current run
    @param continuing: indicates if a previously failed backup is being continued in order to fix
                    the backup series
    @return: path to folder where incremental is to be stored. If the run is not continuing
             it should have hard links to all files of the previous backup already in it
             to speed up synchronization and save space on file systems which do not support
             dedup (like e.g. zfs does).
    """
    logger = Log.instance().logger
    config = configparser.ConfigParser()
    config.read(os.path.join(path_to_backup_series, 'cfg.ini'))

    # read all sections
    last_backup_timestamp: str = get_timestamp_of_last_backup(config)
    logger.info(f"Making incremental backup based on backup from {last_backup_timestamp}.")
    base_path_for_incremental: PathLike[str] = Path(
        os.path.join(os.path.join(config[last_backup_timestamp]['backup'], get_current_series_name()), last_backup_timestamp))

    active_path: PathLike[str] = Path(os.path.join(path_to_backup_series, timestamp))
    logger.info(f"Backup is written to {active_path}.")

    if not continuing:
        shutil.copytree(base_path_for_incremental, active_path, copy_function=os.link)
    with open(os.path.join(path_to_backup_series, 'cfg.ini'), 'w') as configfile:  # save
        config.write(configfile)
    return active_path


def get_timestamp_of_last_backup(config: configparser) -> str:
    """
    Searches in the cfg.ini file of the current backup run for the most recent backup folder and
    returns a string corresponding to the last timestamp.
    @param config: Config parser to current backup runs series ini-file.
    @return: timestamp of last run
    """
    logger = Log.instance().logger
    sections: List[str] = config.sections()
    # we need to convert to datetime, to be able to quickly find the most recent by just
    # applying max ;)
    sec_times: List[datetime] = []
    for sec in sections:
        # in case this is a continuing run, we need to be aware, that the ACTIVE section is still present,
        # as it was not renamed upon completion of the backup.
        if sec == "ACTIVE":
            continue

        try:
            time_stamp_of_series = string_to_datetime(sec)
            sec_times.append(time_stamp_of_series)
        except Exception as e:
            raise Exception(f"Cannot convert found section entry to datetime in order to sort it. Did you rename a backup run? Raised exception reads {str(e)}")

    if not sec_times:
        logger.info("No timestamp found. Must be a continuing run.")
        return ""
    timestamp: str = datetime_to_string(max(sec_times))
    logger.info(f"Took {timestamp} as timestamp for current backup run.")
    return timestamp


def rename_config_section(cfg_parser: configparser, section_from: str, section_to: str):
    """
    Renames a config ini-file section by creating a new one and deleting the old.
    @param cfg_parser: cfg-parser object
    @param section_from: Current section name
    @param section_to: Section name after renaming.
    @attention This does not write the changes to the file!!!
    """
    items = cfg_parser.items(section_from)
    cfg_parser.add_section(section_to)
    for item in items:
        cfg_parser.set(section_to, item[0], item[1])
    cfg_parser.remove_section(section_from)


def backup(timestamp: str, args, runtype: str) -> Tuple[bool, ChangeSummary]:
    """
    Main function handling your backup request.
    @param timestamp: timestamp to identify backup
    @param args: Arguments as parsed by argparser
    @param runtype: either 'full' or 'incr'
    @return: True on success else False
    """
    try:
        logger = Log.instance().logger
        # Let's create this first, as we do not support all parameters yet.
        # This prevents having to clean up the backup if this constructor throws.
        rsync_policy: RsyncPolicy = RsyncPolicy(args.flag)

        incremental: bool = False
        if runtype == 'incr':
            incremental = True

        if not args.destination:
            raise Exception("No destination via the -d flag specified. See --help.")

        if not args.source:
            raise Exception("No sources via the -s flag specified. See --help.")

        if args.remove and args.cont:
            raise Exception("Cannot remove or continue failed backup. Use only one flag as they exclude each other.")

        if incremental:
            logger.info("Running an incremental backup.")
        else:
            logger.info("Running a full backup.")

        config = configparser.ConfigParser()
        config.read(os.path.join(get_path_to_backup_series(args.destination), 'cfg.ini'))
        sources = args.source
        destination = args.destination
        continuing = False  # Indicates that the backup is continuing a previously failed backup.
        if config.has_section('ACTIVE'):
            if config['ACTIVE']['status'] == 'failed' and not args.cont and not args.remove:
                raise Exception("Previous backup failed, either run again with --cont flag enabled, with --remove flag or clean up backup manually.")
            elif config['ACTIVE']['status'] == 'failed' and args.cont:
                timestamp = config["ACTIVE"]['timestamp']
                continuing: bool = True
                with open(os.path.join(get_path_to_backup_series(args.destination), 'cfg.ini'),
                          'w') as configfile:  # save
                    config.write(configfile)

                # if there is only an ACTIVE section, an incremental backup does not make sense and
                # we need to fall back to a ful backup.
                if len(config.sections()) == 1:
                    incremental = False
                    logger.warning("Cannot proceed with incremental backup. Falling back to a filling backup.")

                logger.warning("Run-type changed to a filling backup.")
            elif config['ACTIVE']['status'] == 'failed' and args.remove:
                failed_timestamp = config["ACTIVE"]['timestamp']
                bkp_series_path = os.path.join(config["ACTIVE"]['backup'], get_current_series_name())
                failed_path = os.path.join(bkp_series_path, failed_timestamp)
                logger.warning(f"Backup at {failed_path} will be removed as it failed in a previous run.")
                shutil.rmtree(failed_path, ignore_errors=True)
                if config.has_section("ACTIVE"):
                    config.remove_section("ACTIVE")
                    with open(os.path.join(get_path_to_backup_series(args.destination), 'cfg.ini'),
                              'w') as configfile:
                        config.write(configfile)
                if not config.sections():
                    logger.warning("The failed backup was the backup series full backup. Recreating that.")
                    if incremental:
                        logger.warning("Falling back from incremental to full backup.")
                        incremental = False
            else:
                raise Exception("Previous backup failed or is still active. Can't handle situation :/.\nResolve manually, e.g. by renaming the current series, which will trigger a new series.")

        active_path: PathLike[str] = get_active_backup_path(timestamp, destination, incremental, continuing)
        make_entry_to_ini_for_active_backup(destination, sources, timestamp)
        # actually syncing the data.
        summary, rsync_cmd = RsyncCaller.sync_data(sources, str(active_path), rsync_policy)

        # mark backup as success
        config = configparser.ConfigParser()
        config.read(os.path.join(get_path_to_backup_series(args.destination), 'cfg.ini'))
        config['ACTIVE']['status'] = "complete"
        config['ACTIVE']['rsyncCMD'] = rsync_cmd
        rename_config_section(config, "ACTIVE", timestamp)
        with open(os.path.join(get_path_to_backup_series(args.destination), 'cfg.ini'), 'w') as configfile:  # save
            config.write(configfile)

        create_softlink_to_current_backup(args.link_path,
                                          os.path.join(os.path.join(args.destination, get_current_series_name()),
                                                       timestamp))
        return True, summary

    except Exception as e:
        logger.error(e)
        logger.error('\n' + traceback.format_exc())
        return False, ChangeSummary("")


def create_softlink_to_current_backup(link_path: str, target_symlink_path: str):
    """
    Creates a soft link to the most current backup for which rsync succeeded.
    @param link_path: path to where the softlink shall be created
    @param target_symlink_path: path to the most current backup.
    """
    logger = Log.instance().logger
    if link_path is not None:
        try:
            if os.path.islink(link_path):
                os.unlink(link_path)
            elif os.path.isfile(link_path):
                raise Exception("Path specified for link is a file.")
            elif os.path.isdir(link_path):
                raise Exception("Path specified for link is a directory.")
            Path(link_path).symlink_to(target_symlink_path, target_is_directory=True)
        except Exception as e:
            logger.info(f"Trying to create symlink to {target_symlink_path}")
            logger.error(
                f"Trying to create symlink from '{link_path}' to '{target_symlink_path}'. "
                f"Error in settings, rights or your input caused the following exception: {str(e)}")


def make_entry_to_ini_for_active_backup(destination, sources, timestamp):
    config = configparser.ConfigParser()
    config.read(os.path.join(get_path_to_backup_series(destination), 'cfg.ini'))

    # this may happen if we encounter a failed backup
    if not config.has_section("ACTIVE"):
        config.add_section("ACTIVE")
    config['ACTIVE']['timestamp'] = timestamp
    config['ACTIVE']['status'] = "failed"
    config['ACTIVE']['sources'] = json.dumps(sources)
    config['ACTIVE']['backup'] = str(destination)
    config['ACTIVE']['cwd'] = os.getcwd()
    with open(os.path.join(get_path_to_backup_series(destination), 'cfg.ini'), 'w') as configfile:  # save
        config.write(configfile)


def main():
    parser = argparse.ArgumentParser()
    adding_parser_arguments(parser)
    args, unknown = parser.parse_known_args()

    runtype = "full"
    if args.runtype:
        runtype = args.runtype

    if args.cwd is not None:
        os.chdir(Path(args.cwd))
        print(os.getcwd())

    log_path: Path = Path("logs/")
    if args.log_destination is not None:
        log_path = Path(args.log_destination)

    log_path.mkdir(parents=True, exist_ok=True)
    logger = Log.instance().logger
    now = datetime.datetime.now()
    timestamp = datetime_to_string(now)
    set_up_logger(args.log_destination, timestamp)
    LogZipper.zip_log_files_from_previous_runs(args.log_destination, os.path.join(args.log_destination, f"{timestamp}.log"))
    success, summary = backup(timestamp, args, runtype)

    exit_code = 0
    if success:
        if logger.error.counter == 0:
            logger.info(
                f"Backup terminated successfully, {logger.warning.counter} warnings and {logger.error.counter} errors.")
        else:
            logger.info(
                f"Backup encountered errors, but reached a successful state. {logger.warning.counter} warnings and {logger.error.counter} errors.")
    else:
        logger.error(
            f"Backup terminated with errors, {logger.warning.counter} warnings and {logger.error.counter} errors.")
        exit_code = 1

    Log.instance().print_log_summary()
    logging.shutdown()

    LogZipper.zip_log_files_from_previous_runs(args.log_destination)
    sys.exit(exit_code)


def adding_parser_arguments(parser):
    parser.add_argument('-t', '--runtype', help="Specify 'full' or 'incr', default: 'full'")
    parser.add_argument('-d', '--destination', help="Path to destination")
    parser.add_argument('-l', '--log_destination', default='logs', help="Path to log files to be used.")
    parser.add_argument('--link_path', help="Specify a path to a symbolic link to be created pointing to the most"
                                            "recent backup.")
    parser.add_argument('-c', '--cont', action='store_true', help="cont == continue: If a backup is interrupted the"
                                                                  "backups status is marked failed, the increment "
                                                                  "would build on a failed predecessor. When cont is "
                                                                  "specified it will finish the last backup first and "
                                                                  "only then will it continue making a new backup.")
    parser.add_argument('-w', '--cwd', help="Path specify a path in which the program shall execute. CWD.")
    parser.add_argument('-r', '--remove', action='store_true', help="Removes failed backup and starts clean.")
    parser.add_argument('-s', '--source', action='append', help="Specify a source")
    parser.add_argument('-f', '--flag', action='append', metavar='rsync_flag', help='Flag to be be passed to rsync. '
                                                                                    'Use like this -f --delete, '
                                                                                    'to pass --delete to rsync')


if __name__ == '__main__':
    main()
