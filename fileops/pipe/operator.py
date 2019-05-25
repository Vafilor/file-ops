from multiprocessing import Queue
import datetime


class TerminateOperand:
    """Used to signal to stop processing pipe data."""
    def __init__(self, when=None):
        if when is None:
            when = datetime.datetime.utcnow()

        self.when = when


class Operator:
    """An InputOutputOperator takes input from a Queue, does work on it, and outputs the results into another Queue"""
    def __init__(self):
        pass

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        item = input_queue.get()
        while not isinstance(item, TerminateOperand):
            output_queue.put(item)
            item = input_queue.get()

        # Put it back so other operators get the Termination signal
        input_queue.put(item)