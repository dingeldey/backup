from submodules.python_core_libs.logging.project_logger import Log
from utils.rsyncpolicy import RsyncPolicy
from typing import List, Tuple
import subprocess
import os
from utils.changesummary import ChangeSummary


class RsyncCaller:
    @staticmethod
    def sync_data(sources: List[str], active_backup_path: str, rsync_policy: RsyncPolicy) -> Tuple[ChangeSummary, str]:
        """
        Making the actual rsync call.
        @param sources: List of source paths
        @param active_backup_path: backup path for the current timestamp.
        @param rsync_policy: Policy in which the parameters of the rsync call are assembled.
        @return: 1) Change summary of rsync. Can be used to see if a really large amount of files was removed.
                 2) Used rsync cmd
        """
        logger = Log.instance().logger
        is_not_nt_like: bool = os.name != 'nt'

        if is_not_nt_like:
            RsyncCaller.check_if_sources_are_empty(sources)
            logger.info(f"Mirroring {sources} to {active_backup_path}.")
            rsync_cmd = "rsync "
            for flag in rsync_policy.parameters:
                rsync_cmd = rsync_cmd + " " + flag
            for source in sources:
                rsync_cmd = rsync_cmd + " " + source

            rsync_cmd = rsync_cmd + " " + active_backup_path
            logger.info(f"rsync command reads: {rsync_cmd}")
            out = ""
            with subprocess.Popen(['rsync', *rsync_policy.parameters, *sources, active_backup_path],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1,
                                  universal_newlines=True) as p:
                for line in p.stdout:
                    out += line
                    logger.info(line.strip('\n'))  # process line here

            summary: ChangeSummary = ChangeSummary(out)
            return summary, rsync_cmd

        else:
            RsyncCaller.check_if_sources_are_empty(sources)
            logger.info(f"Mirroring {sources} to {active_backup_path}.")

            # converting paths to wsl-paths
            wsl_sources: List[str] = []
            for source_path in sources:
                wsl_sources.append(subprocess.check_output(
                    ['wsl', 'wslpath', str(os.path.abspath(source_path)).replace(os.sep, '/')]).decode("UTF-8").strip(
                    "\n"))

            # WSL has different absolut path. These commands will determine it and apply it accordingly.
            backup_wsl_path = subprocess.check_output(
                ['wsl', 'wslpath', str(os.path.abspath(active_backup_path)).replace(os.sep, '/')]).decode(
                "UTF-8").strip(
                "\n")
            logger.info(
                f"[WINDOWS] Converted source path to WSL path to {wsl_sources} and backup path became {backup_wsl_path}.")

            rsync_cmd = "rsync "
            for flag in rsync_policy.parameters:
                rsync_cmd = rsync_cmd + " " + flag
            for source in wsl_sources:
                rsync_cmd = rsync_cmd + " " + source

            rsync_cmd = rsync_cmd + " " + backup_wsl_path
            logger.info(f"rsync command reads: {rsync_cmd}")
            out = ""
            with subprocess.Popen(['wsl', 'rsync', *rsync_policy.parameters, *wsl_sources, backup_wsl_path],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, bufsize=1, universal_newlines=True) as p:
                for line in p.stdout:
                    out += line
                    logger.info(line.strip('\n'))  # process line here

            summary: ChangeSummary = ChangeSummary(out)
            return summary, rsync_cmd

    @staticmethod
    def check_if_sources_are_empty(sources: List[str]):
        """
        Checks if sources are not empty. If it encounters an empty source it raises an
        exception to cause the backup to fail.
        @param sources: List of source paths.
        """
        for source in sources:
            if os.path.isfile(source):
                continue

            if not os.path.isdir(source) or os.path.isfile(source):
                raise Exception("Specified source does not exist.")

            # check if empty
            if not any(os.scandir(source)):
                raise Exception("Directory is empty. This causes a backup to fail")

