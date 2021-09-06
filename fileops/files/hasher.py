import hashlib

from concurrent.futures import ThreadPoolExecutor, Future
from multiprocessing import Queue
from fileops.pipe.operator import Operator, TerminateOperand
from threading import Lock, Condition
from enum import Enum
from typing import Optional, Tuple

from fileops.files.file import File


class HasherProgressReporter:
    """
    Reports Hashing file progress.
    """
    class State(Enum):
        Started = 0
        Finished = 1
        Failed = 2

    def submit_file(self, file: File, state, exception: Optional[BaseException] = None):
        """
        Gives a file that is to be hashed and it's current state.
        If the state is Failed, message should contain a reason why it failed.

        This method may be called from different threads.
        """
        raise NotImplementedError()


class PrintHasherProgress(HasherProgressReporter):
    def submit_file(self, file: File, state: HasherProgressReporter.State, exception: Optional[BaseException] = None):
        state_string = 'Started: '

        if state == HasherProgressReporter.State.Failed:
            state_string = 'Failed: '
        elif state == HasherProgressReporter.State.Finished:
            state_string = 'Finished: '

        print(f'{state_string} {file.path}')


class Hasher(Operator):
    """
    Calculates the md5 hash of files coming in and outputs them.
    Hashes are calculated on separate threads.
    Directories are skipped.

    Override the process_file method to change the hashing algorithm.
    """

    # Bytes to read at a time while calculating md5 hash
    BytesToRead = 1048576

    def __init__(self, progress: Optional[HasherProgressReporter] = None):
        """
        max_workers:
            The number of threads we should use while hashing files.
        """
        Operator.__init__(self)
        self.output_queue = None
        self.progress = progress

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        """
        input_queue:
            A Queue of File(s).

        output_queue:
            A Queue of File(s) that have their content_hash calculated.
        """

        file = input_queue.get()
        while not isinstance(file, TerminateOperand):
            try:
                is_directory = file.is_directory
            except FileNotFoundError:
                # todo error log
                print(f'{file.path} not found')
                file = input_queue.get()
                continue

            if is_directory:
                output_queue.put(file)
            else:
                out_file, err = self.process_file(file)
                if err:
                    print(err)

                output_queue.put(out_file)

            file = input_queue.get()

        output_queue.put(TerminateOperand())

    def process_file(self, file: File) -> Tuple[File, Optional[BaseException]]:
        """
        Calculates the hash of the file, assumed to not be a directory.
        This method calculates the md5 hash. On failure, the hash is set to the empty string.

        Override this method to calculate a different hash.
        """

        hash_md5 = hashlib.md5()
        try:
            with open(file.path, "rb") as f:
                for chunk in iter(lambda: f.read(Hasher.BytesToRead), b""):
                    hash_md5.update(chunk)

            file.content_hash = hash_md5.hexdigest()
        except OSError as ex:
            file.content_hash = ''
            return file, ex

        return file, None
