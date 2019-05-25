from multiprocessing import Queue
from ..pipe.operator import Operator, TerminateOperand


class Printer(Operator):
    """Prints items from a queue to the console"""
    def __init__(self, formatter=None):
        """
        Formatter must be a class with a format(item) method.

        We can't use a lambda due to multiprocessing not being able to pickle lambdas.
        """
        Operator.__init__(self)
        self.formatter = formatter

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        """
        Takes each item from the input_queue and prints it to the console using the formatter.

        If no formatter was specified, the item is just passed to the print function.
        """
        item = input_queue.get()
        while not isinstance(item, TerminateOperand):
            if self.formatter is not None:
                formatted = self.formatter.format(item)
            else:
                formatted = f'{item}\n'

            print(formatted)

            output_queue.put(item)
            item = input_queue.get()

        # Put the TerminateOperand back so other consumers get it.
        input_queue.put(item)
