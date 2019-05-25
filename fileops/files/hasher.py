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

    def __init__(self, max_workers: int = 10, progress: Optional[HasherProgressReporter] = None):
        """
        max_workers:
            The number of threads we should use while hashing files.
        """
        Operator.__init__(self)
        self.max_workers = max_workers
        self.output_queue = None
        self.futures = {}
        self.lock = None
        self.finish_condition = None
        self.progress = progress
        self.executor = None

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        """
        input_queue:
            A Queue of File(s).

        output_queue:
            A Queue of File(s) that have their content_hash calculated.
        """
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

        self.lock = Lock()
        self.finish_condition = Condition(self.lock)
        self.output_queue = output_queue
        self.futures = {}

        file = input_queue.get()
        while not isinstance(file, TerminateOperand):
            if file.is_directory:
                output_queue.put(file)
            else:
                self.submit_file(self.executor, file)

            file = input_queue.get()

        # Put terminator back on queue
        input_queue.put(file)

        with self.finish_condition:
            self.finish_condition.wait_for(self.is_done)

        self.executor.shutdown()

    def is_done(self):
        """
        Returns True if there are no more files to hash.
        """
        return len(self.futures.keys()) == 0

    def submit_file(self, executor: ThreadPoolExecutor, file: File):
        """
        Submits the file to have its hash calculated. On done, removes itself from the pending/hashing files.
        """
        self.report_progress(file, HasherProgressReporter.State.Started)
        future = executor.submit(self.process_file, file)

        with self.lock:
            self.futures[file.path] = future

        future.add_done_callback(self.finish_file)

    def finish_file(self, future: Future):
        """
        Finishes processing the hashed file, removes it from the pending/hashing files list,
        and puts it on the output queue.
        """
        file, exception = future.result()

        with self.lock:
            del self.futures[file.path]

        self.output_queue.put(file)

        if exception is not None:
            self.report_progress(file, HasherProgressReporter.State.Failed, exception)
        else:
            self.report_progress(file, HasherProgressReporter.State.Finished)

        with self.finish_condition:
            self.finish_condition.notify()

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

    def report_progress(self, file: File, state: HasherProgressReporter.State, message: Optional[BaseException] = None):
        if self.progress is not None:
            self.progress.submit_file(file, state, message)
