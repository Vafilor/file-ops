from multiprocessing import Queue
from ..pipe.operator import Operator


class Counter(Operator):
    """Prints items from a queue to the console once the buffer_size is reached."""
    def __init__(self, chunk_size=100, formatter=None):
        """
        Formatter must be a class with a format(item) method.

        We can't use a lambda due to multiprocessing not being able to pickle lambdas.
        """
        super().__init__()
        self.chunk_size = chunk_size
        self.count = 0
        self.formatter = formatter

    def print_count(self):
        if self.formatter is not None:
            formatted = self.formatter.format(self.count)
        else:
            formatted = f'{self.count}\n'

        print(formatted)

    def _initialize(self, input_queue: Queue, output_queue: Queue):
        super()._initialize(input_queue, output_queue)
        self.count = 0

    def _action(self, item, output_queue: Queue) -> None:
        """
        Takes each item from the input_queue and prints it to the console using the formatter.

        If no formatter was specified, the item is just passed to the print function.
        """
        output_queue.put(item)

        self.count += 1
        if self.count % self.chunk_size == 0:
            self.print_count()

    def _finish(self, input_queue: Queue, output_queue: Queue) -> None:
        super()._finish(input_queue, output_queue)
        self.print_count()
