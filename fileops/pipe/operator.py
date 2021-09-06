from multiprocessing import Queue
import datetime


class TerminateOperand:
    """Used to signal to stop processing pipe data."""
    def __init__(self, when=None):
        if when is None:
            when = datetime.datetime.utcnow()

        self.when = when

    def __eq__(self, other):
        if not isinstance(other, TerminateOperand):
            return False

        # All TerminateOperands are equal to each other.
        return True


class Operator:
    """An Operator takes input from a Queue, does work on it, and outputs the results into another Queue"""
    def __init__(self):
        pass

    def _action(self, item, output_queue: Queue) -> None:
        output_queue.put(item)

    def _initialize(self, input_queue: Queue, output_queue: Queue) -> None:
        pass

    def _process_actions(self, input_queue: Queue, output_queue: Queue) -> None:
        for item in iter(input_queue.get, TerminateOperand()):
            self._action(item, output_queue)

    def _finish(self, input_queue: Queue, output_queue: Queue) -> None:
        pass

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        self._initialize(input_queue, output_queue)
        self._process_actions(input_queue, output_queue)
        self._finish(input_queue, output_queue)
