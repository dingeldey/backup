class ChangeSummary:
    """
    Change summary of the last backup run.
    """

    def __init__(self, rsync_summary_string: str):
        """
        @param rsync_summary_string: Result string from rsync command.
        """
        self.__num_changes_tot = 0
        self.__num_changes_directories = 0
        self.__num_changes_files = 0

        if not rsync_summary_string:
            return
        if not rsync_summary_string.find("sending incremental file list") < 0 and rsync_summary_string.find(
                "(DRY RUN)") < 0:
            self.__num_changes_tot = rsync_summary_string.count('\n') - 4
            self.__num_changes_directories = rsync_summary_string.count('/\n')
            self.__num_changes_files = self.__num_changes_tot - self.__num_changes_directories

    @property
    def get_summary(self):
        """
        @return: A summary string of the changes performed.
        """
        return f"Counted {self.__num_changes_tot} changes to last backup with {self.__num_changes_directories} " \
               f"directories and {self.__num_changes_files} files involved. "
