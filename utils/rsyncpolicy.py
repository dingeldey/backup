from typing import List


class RsyncPolicy:
    """
    Copy policy. Specifying rsync parameters.
    """
    def __init__(self, rsync_params: List[str]):
        """
        Constructor may raise an exception when encountering an unsupported flag.
        @param rsync_params: parameter list to be used in rsync command
        """
        self.__rsync_params: List[str] = []
        if rsync_params is not None:
            self.__rsync_params: List[str] = rsync_params
            # check for not supported parameters
            for para in self.__rsync_params:
                if not para.find("--dry-run") < 0:
                    raise Exception("rsync parameter '--dry-run' not supported.")

    @property
    def parameters(self) -> List[str]:
        return self.__rsync_params
