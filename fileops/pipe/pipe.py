from multiprocessing import Queue, Process
from threading import Thread

from .operator import TerminateOperand


class NullQueue:
    """
    A Queue that does nothing.
    Used as part of a pipe to denote a starting and endpoint point.
    """
    def get(self, *args, **kwargs):
        return []

    def put(self, *args, **kwargs):
        pass


class Pipe:
    """
    A Pipe is a flow of data. Each part of the pipe works on the data, and passes it to the next part.

    The first part of the pipe should produce data for the next parts.
    You can have multiple Operators for each part of the pipe.
    E.g. multiple producers for the first part of the pipe and multiple consumers at each part of the pipe.

    Pipes use Queues to communicate the data between each part of the pipe.
    As each part of the pipe is done, a TeminateOperand is put on the queue to signal it.

    It is expected that Operands put the TerminateOperand back on the input_queue when they run into it
    so we can support multiple consumers/producers.

    The default Pipe class runs each part of the pipe sequentially, so multiple consumers/producers don't provide
    any benefit.
    """
    def __init__(self):
        self.queues = []
        self.operators = []  # This is a list of lists

    def pipe(self, *operators):
        """
        Adds a component to the pipe. Operators can be a list of items, all must subclass Operator.
        """
        self.operators.append(operators)

        return self

    def run(self):
        """
        Runs the pipe operands in sequence.
        """
        self.queues.append(NullQueue())
        for i in range(0, len(self.operators) - 1):
            self.queues.append(Queue())
        self.queues.append(NullQueue())

        index = -1

        for ops in self.operators:
            index += 1
            current_queue = self.queues[index]
            next_queue = self.queues[index + 1]

            for op in ops:
                op.process(current_queue, next_queue)

            next_queue.put(TerminateOperand())


class ProcessPipe(Pipe):
    """Runs each part of the pipe in its own process. This makes multiple producers/consumers beneficial."""
    def run(self):
        self.queues.append(NullQueue())
        for i in range(0, len(self.operators) - 1):
            self.queues.append(Queue())
        self.queues.append(NullQueue())

        processes = []
        index = -1

        for ops in self.operators:
            index += 1
            current_queue = self.queues[index]
            next_queue = self.queues[index + 1]

            chunk = []

            for op in ops:
                process = Process(target=op.process, args=(current_queue, next_queue,))
                chunk.append(process)
                process.start()

            processes.append(chunk)

        # Skip first null queue
        index = 0
        for process_chunk in processes:
            index += 1
            for process in process_chunk:
                process.join()

            self.queues[index].put(TerminateOperand())


class ThreadPipe(Pipe):
    """Runs each part of the pipe in its own Thread. This makes multiple producers/consumers beneficial."""
    def run(self):
        self.queues.append(NullQueue())
        for i in range(0, len(self.operators) - 1):
            self.queues.append(Queue())
        self.queues.append(NullQueue())

        threads = []
        index = -1

        for ops in self.operators:
            index += 1
            current_queue = self.queues[index]
            next_queue = self.queues[index + 1]

            chunk = []

            for op in ops:
                thread = Thread(target=op.process, args=(current_queue, next_queue,))
                chunk.append(thread)
                thread.start()

            threads.append(chunk)

        # Skip first null queue
        index = 0
        for thread_chunk in threads:
            index += 1
            for thread in thread_chunk:
                thread.join()

            self.queues[index].put(TerminateOperand())
