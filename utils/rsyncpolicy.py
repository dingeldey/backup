from typing import List


class RsyncPolicy:
    """
    Copy policy. Specifying rsync parameters.
    """
    def __init__(self, rsync_params: List[str]):
        self.__rsync_params: List[str] = rsync_params

    @property
    def flags(self) -> List[str]:
        return self.__rsync_params
