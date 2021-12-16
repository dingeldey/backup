class RsyncPolicy:
    """
    Copy policy. Specifying rsync parameters.
    """

    def __init__(self, use_checksum: bool):
        """
        @param use_checksum: Makes rsync compare checksums. Significant slow down.
        """
        self.__use_checksum: bool = use_checksum

    @property
    def flags(self):
        flags_value = "-av"
        if self.use_checksum:
            flags_value = flags_value + 'c'
        return flags_value

    @property
    def use_checksum(self) -> bool:
        return self.__use_checksum
