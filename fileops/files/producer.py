import os

from os.path import join
from multiprocessing import Queue
from typing import Union, Optional

from .file import File
from ..pipe.operator import Operator


class FileProgressReporter:
    """
    Keep track of a FileProducer's progress.
    """
    def submit_file(self, file: File):
        """
        Called whenever a FileProducer walks over a file.
        """
        raise NotImplementedError()


class PrintFileProgress(FileProgressReporter):
    def submit_file(self, file: File):
        print(f'File: {file.path}')


class FileProducer(Operator):
    """
    Gets all files and directories under a path. Outputs the files as File objects to a queue.
    """
    def __init__(self, path: Union[bytes, str], progress_reporter: Optional[FileProgressReporter] = None):
        Operator.__init__(self)
        self.path = path
        self.progress_reporter = progress_reporter

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        for root, dirs, files in os.walk(self.path):
            for directory in dirs:
                file = File(path=join(root, directory), is_directory=True)
                output_queue.put(file)
                self.report_file(file)

            for name in files:
                file = File(path=join(root, name), is_directory=False)
                output_queue.put(file)
                self.report_file(file)

    def report_file(self, file: File):
        if self.progress_reporter is not None:
            self.progress_reporter.submit_file(file)
