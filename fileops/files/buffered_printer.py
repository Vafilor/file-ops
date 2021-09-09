from multiprocessing import Queue
from ..pipe.operator import Operator, TerminateOperand


class BufferedPrinter(Operator):
    """Prints items from a queue to the console once the buffer_size is reached."""
    def __init__(self, buffer_size=100, formatter=None):
        """
        Formatter must be a class with a format(item) method.

        We can't use a lambda due to multiprocessing not being able to pickle lambdas.
        """
        Operator.__init__(self)
        self.buffer_size = buffer_size
        self.buffer = ''
        self.buffer_count = 0
        self.formatter = formatter

    def add_buffer(self, value: str):
        self.buffer += value + "\n"
        self.buffer_count += 1

    def flush_buffer(self):
        if self.buffer != '':
            print(self.buffer)

        self.buffer_count = 0
        self.buffer = ''

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        """
        Takes each item from the input_queue and prints it to the console using the formatter.

        If no formatter was specified, the item is just passed to the print function.
        """
        item = input_queue.get()
        while not isinstance(item, TerminateOperand):
            output_queue.put(item)

            if self.formatter is not None:
                formatted = self.formatter.format(item)
            else:
                formatted = f'{item}\n'

            self.add_buffer(formatted)

            if self.buffer_count >= self.buffer_size:
                self.flush_buffer()

            item = input_queue.get()

        self.flush_buffer()

        output_queue.put(item)
