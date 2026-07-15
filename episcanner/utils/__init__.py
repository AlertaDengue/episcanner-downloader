import os
import pathlib

from ._brasil import *  # noqa

CACHEPATH = os.getenv(
    "EPISCANNER_CACHEPATH",
    os.path.join(str(pathlib.Path.home()), "episcanner"),
)

__cachepath__ = pathlib.Path(CACHEPATH)
__cachepath__.mkdir(exist_ok=True)
