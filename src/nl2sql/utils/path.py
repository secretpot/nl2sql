import os
from typing import AnyStr


def fpd(file: os.PathLike[AnyStr] = None, depth: int = 1) -> str:
    """
    get file parent directory
    """
    file = file or __file__
    p = os.path.dirname(os.path.abspath(file))
    for _ in range(depth - 1):
        p = os.path.dirname(p)
    return p
