import os
import zipfile
from pathlib import Path
import glob
from typing import List


class LogZipper:
    @staticmethod
    def zip_log_files_from_previous_runs(log_destination: str, exclude_file: str = ""):
        """
        @param log_destination: directory of log files.
        @param exclude_file: Specifiy log file to exclude, leave empty if non shall be excluded
        """
        unzipped_log_files: List = glob.glob(os.path.join(log_destination, "*.log"))
        for log_file in unzipped_log_files:
            if log_file == exclude_file:
                continue
            zf = zipfile.ZipFile(Path(str(log_file) + '.zip'), mode='w')
            try:
                zf.write(log_file, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
            finally:
                zf.close()
            os.remove(log_file)
