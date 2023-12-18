from pathlib import Path

import PyInstaller.__main__

SRC = Path(__file__).parent
MAIN = SRC / "scanner" / "__main__.py"


def install():
    PyInstaller.__main__.run(
        [str(MAIN.absolute()), "--onefile", "--windowed", "--name=episcanner"]
    )
