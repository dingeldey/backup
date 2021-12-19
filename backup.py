import argparse
import json
import shutil
import os.path
import traceback
import configparser
from os import PathLike
from pathlib import Path
from submodules.python_core_libs.logging.project_logger import Log
from typing import List
import subprocess
from utils.rsyncpolicy import RsyncPolicy
from utils.loggerutils import *
from utils.datetimeutils import *


def get_path_to_backup_series(destination_path: PathLike) -> PathLike:
    """
    @param destination_path: Backup path
    @return: Returns the backup-path with a simple addition to indicate where the current backup-series is being placed.
             I chose '0' as this places the current backup in from of the folder and makes it easy to identify.
    """
    return Path(os.path.join(destination_path, Path("0")))


def get_active_backup_path(timestamp: str, destination_path: PathLike, incremental: bool, filling: bool) \
        -> PathLike:
    """
    Create a folder to backup to as well as moving old backups

    @param timestamp: time stamp of the current run.
    @param destination_path: Path of backup root folder
    @param incremental: Indicates if an incremental backup is wanted.
    @param filling: Indicates that a previously failed backup is continued.
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
        return incremental_backup(path_to_backup_series, timestamp, filling)
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
        logger.info(f"Folder {active_path} already exists. This is probably to a run fill run.")
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


def incremental_backup(path_to_backup_series: PathLike, timestamp: str, filling: bool) -> PathLike:
    """
    Creates or returns (when filling) active folder for incremental backup.
    @param path_to_backup_series: Backup series folder where all incrementals are saved.
    @param timestamp: timestamp of current run
    @param filling: indicates if a previously failed backup is being filled in order to fix
                    the backup series
    @return: path to folder where incremental is to be stored. If the run is not filling
             it should have hard links to all files of the previous backup already in it
             to speed up synchronization and save space on file systems which do not support
             dedup (like e.g. zfs does).
    """
    logger = Log.instance().logger
    config = configparser.ConfigParser()
    config.read(os.path.join(path_to_backup_series, 'cfg.ini'))

    # read all sections
    last_backup: str = get_timestamp_of_last_backup(config)
    logger.info(f"Making incremental backup based on backup from {last_backup}.")
    base_path_for_incremental: PathLike[str] = Path(os.path.join(config[last_backup]['backup'], last_backup))
    active_path: PathLike[str] = Path(os.path.join(path_to_backup_series, timestamp))
    logger.info(f"Backup is written to {active_path}.")

    if not filling:
        shutil.copytree(base_path_for_incremental, active_path, copy_function=os.link)
    with open(os.path.join(path_to_backup_series, 'cfg.ini'), 'w') as configfile:  # save
        config.write(configfile)
    return active_path


def get_timestamp_of_last_backup(config: configparser) -> str:
    """
    Searches in the cfg.ini file of the current backup run for the most recent backup folder and
    returns a string corresponding to the last timestamp.
    @param config:
    @return:
    """
    logger = Log.instance().logger
    sections: List[str] = config.sections()
    # we need to convert to datetime, to be able to quickly find the most recent by just
    # applying max ;)
    sec_times: List[datetime] = []
    for sec in sections:
        # in case this is a filling run, we need to be aware, that the ACTIVE section is still present,
        # as it was not renamed upon completion of the backup.
        if sec == "ACTIVE":
            continue
        sec_times.append(string_to_datetime(sec))

    if not sec_times:
        logger.info("No timestamp found. Must be a filling run.")
        return ""
    timestamp: str = datetime_to_string(max(sec_times))
    logger.info(f"Took {timestamp} as timestamp for current backup run.")
    return timestamp


def sync_data(sources: List[str], active_backup_path: str, rsync_policy: RsyncPolicy):
    """
    Making the actual rsync call.
    @param sources: List of source paths
    @param active_backup_path: backup path for the current timestamp.
    @param rsync_policy: Policy in which the parameters of the rsync call are assembled.
    """
    logger = Log.instance().logger
    is_not_nt_like: bool = os.name != 'nt'

    if is_not_nt_like:
        for source_path in sources:
            logger.info(f"Mirroring {source_path} to {active_backup_path}.")
            out = subprocess.check_output(["rsync", rsync_policy.flags, "--delete", source_path, active_backup_path, "-p"])
            logger.info(out.decode("utf-8"))
    else:
        for source_path in sources:

            logger.info(f"Mirroring {source_path} to {active_backup_path}.")
            # WSL has different absolut path. These commands will determine it and apply it accordingly.
            source_wsl_path = subprocess.check_output(['wsl', 'wslpath', str(os.path.abspath(source_path)).replace(os.sep, '/')]).decode("UTF-8").strip("\n")
            backup_wsl_path = subprocess.check_output(['wsl', 'wslpath', str(os.path.abspath(active_backup_path)).replace(os.sep, '/')]).decode("UTF-8").strip("\n")
            logger.info(f"[WINDOWS] Converted source path to WSL path to {source_wsl_path} and backup path became {backup_wsl_path}.")
            out = subprocess.check_output(['wsl', 'rsync',  rsync_policy.flags, "--delete", '-p', source_wsl_path, backup_wsl_path]).decode("UTF-8")

            logger.info(out)


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


def backup(timestamp: str, args):
    """
    Main function handling your backup request.
    @param timestamp: timestamp to identify backup
    """
    logger = Log.instance().logger
    incremental: bool = args.incremental
    if not args.destination:
        raise Exception("No destination via the -d flag specified. See --help.")

    if not args.sources:
        raise Exception("No sources via the -s flag specified. See --help.")

    if args.remove and args.fill:
        raise Exception("Cannot remove and fill failed backup. Use only one flag as they exclude each other.")

    if args.incremental:
        logger.info("Running an incremental backup.")
    else:
        logger.info("Running a full backup.")

    try:
        config = configparser.ConfigParser()
        config.read(os.path.join(get_path_to_backup_series(args.destination), 'cfg.ini'))
        sources = args.sources
        destination = args.destination
        filling = False     # Indicates that the backup is filling a previously failed backup.
        if config.has_section('ACTIVE'):
            if config['ACTIVE']['status'] == 'failed' and not args.fill and not args.remove:
                raise Exception("Previous backup failed, either run again with fill flag enabled, with remove flag or "
                                "clean up backup manually.")
            elif config['ACTIVE']['status'] == 'failed' and args.fill:
                timestamp = config["ACTIVE"]['timestamp']
                filling = True
                with open(os.path.join(get_path_to_backup_series(args.destination), 'cfg.ini'),
                          'w') as configfile:  # save
                    config.write(configfile)

                logger.info("Run continues as a filling run of a previously failed backup.")
            elif config['ACTIVE']['status'] == 'failed' and args.remove:
                failed_timestamp = config["ACTIVE"]['timestamp']
                bkp_series_path = config["ACTIVE"]['backup']
                failed_path = os.path.join(bkp_series_path, failed_timestamp)
                logger.warning(f"Removing failed backup with timestamp {failed_timestamp} at {failed_path}.")
                shutil.rmtree(failed_path)
                if config.has_section("ACTIVE"):
                    config.remove_section("ACTIVE")
                    with open(os.path.join(get_path_to_backup_series(args.destination), 'cfg.ini'),
                              'w') as configfile:  # save
                        config.write(configfile)
                if not config.sections():
                    logger.warning("The failed backup was the backup series full backup. Recreating that.")
                    if args.incremental:
                        logger.warning("Falling back from incremental to full backup.")
                        incremental = False
            else:
                raise Exception("Previous backup failed or is still active. Can't handle situation :/")

        active_path: PathLike[str] = get_active_backup_path(timestamp, destination, incremental, filling)
        make_entry_to_ini_for_active_backup(destination, sources, timestamp)
        rsync_policy: RsyncPolicy = RsyncPolicy(args.checksum)
        sync_data(sources, active_path, rsync_policy)

        # mark backup as success
        config = configparser.ConfigParser()
        config.read(os.path.join(get_path_to_backup_series(args.destination), 'cfg.ini'))
        config['ACTIVE']['status'] = "complete"
        rename_config_section(config, "ACTIVE", timestamp)
        with open(os.path.join(get_path_to_backup_series(args.destination), 'cfg.ini'), 'w') as configfile:  # save
            config.write(configfile)

    except Exception as e:
        logger.error(e)
        logger.error('\n' + traceback.format_exc())


def make_entry_to_ini_for_active_backup(destination, sources, timestamp):
    config = configparser.ConfigParser()
    config.read(os.path.join(get_path_to_backup_series(destination), 'cfg.ini'))

    # this may happen if we encounter a failed backup
    if not config.has_section("ACTIVE"):
        config.add_section("ACTIVE")
    config['ACTIVE']['timestamp'] = timestamp
    config['ACTIVE']['status'] = "failed"
    config['ACTIVE']['sources'] = json.dumps(sources)
    config['ACTIVE']['backup'] = str(get_path_to_backup_series(destination))
    config['ACTIVE']['cwd'] = os.getcwd()
    with open(os.path.join(get_path_to_backup_series(destination), 'cfg.ini'), 'w') as configfile:  # save
        config.write(configfile)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--incremental', action='store_true', help="Indicate an incremental backup is desired.")
    parser.add_argument('-d', '--destination', help="Path to destination")
    parser.add_argument('-l', '--log_destination', default='logs', help="Path to log files to be used.")
    parser.add_argument('-f', '--fill', action='store_true',
                        help="If a backup is interrupted the backups status is marked failed, the increment "
                             "would build on a failed predecessor. When fill "
                             "is specified it will finish the last backup first and only then will it "
                             "continue making a new backup.")
    parser.add_argument('-r', '--remove', action='store_true', help="Removes failed backup and starts clean.")
    parser.add_argument('-s', '--sources', nargs='+', default=[], help="List of sources, comma separated.")
    parser.add_argument('-c', '--checksum', action='store_true',
                        help="Tell rsync to use checksums before copying file.")
    args, unknown = parser.parse_known_args()

    log_path: PathLike[str] = Path("logs/")
    log_ini_path: PathLike = Path(os.path.join(log_path, "logger.ini"))

    remove_logger_ini(log_ini_path)
    logger = Log.instance().logger
    now = datetime.datetime.now()
    timestamp = datetime_to_string(now)
    if not os.path.isdir(args.log_destination):
        os.mkdir(args.log_destination)

    for source in args.sources:
        if os.path.isfile(source) or source[-1] == "/" or source[-1] == os.sep or source[-1] == "\\\\":
            raise Exception('Backup currently only supports backing up directories not single files. Make sure that '
                            'source does not end with path seperator.')

    logfile_path: PathLike[str] = Path(os.path.join(args.log_destination, Path(timestamp + '.log')))
    if os.path.isfile(logfile_path):
        raise Exception("You are triggering to program to quickly, wait at least a second as the timestamps only have "
                        "a one second resolution")
    create_logger_ini(log_ini_path, logfile_path)
    Log.instance().set_ini(log_ini_path)
    logger.info(f"Writing log to {logfile_path}.")
    remove_logger_ini(log_ini_path)

    try:
        backup(timestamp, args)
    except Exception as e:
        logger.error(e)
        logger.error('\n' + traceback.format_exc())


if __name__ == '__main__':
    main()
