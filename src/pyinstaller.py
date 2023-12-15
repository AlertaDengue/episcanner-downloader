import PyInstaller.__main__
from pathlib import Path

SRC = Path(__file__).parent
MAIN = SRC / "scanner" / "__main__.py"


def install():
    PyInstaller.__main__.run([
        str(MAIN.absolute()),
        '--onefile',
        '--windowed',
        '--name=episcanner'
    ])
