import os
import pathlib

from os.path import join
from multiprocessing import Queue

from .file import File
from ..pipe.operator import Operator


class FileProducer(Operator):
    """
    Gets all files and directories under a path. Outputs the files as File objects to a queue.
    """
    def __init__(self, path: pathlib.Path):
        super().__init__()
        self.path = path

    def _process_actions(self, input_queue: Queue, output_queue: Queue):
        for root, dirs, files in os.walk(self.path):
            for directory in dirs:
                file = File(path=join(root, directory), is_directory=True)
                output_queue.put(file)

            for name in files:
                file = File(path=join(root, name), is_directory=False, eager=True)
                output_queue.put(file)
