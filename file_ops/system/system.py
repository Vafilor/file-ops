import os
import subprocess
import sys
from pathlib import Path


def open_file(path: Path) -> None:
    abs_path = str(path.absolute())
    if sys.platform.startswith("darwin"):  # Mac
        subprocess.run(["open", abs_path])
    elif os.name == "nt":  # Windows
        os.startfile(abs_path)
    else:
        subprocess.run(["xdg-open", abs_path])


def open_directory(path: Path) -> None:
    abs_path = str(path.absolute())
    if sys.platform.startswith("darwin"):  # Mac
        subprocess.run(["open", abs_path])
    elif os.name == "nt":  # Windows
        if path.is_file():
            # Open the file in explorer and highlight it
            subprocess.run(["explorer", "/select,", abs_path])
        else:
            os.startfile(abs_path)
    else:
        subprocess.run(["xdg-open", abs_path])
