from multiprocessing import Queue

from fileops.pipe.operator import Operator, TerminateOperand


class DeletedFileFilter(Operator):
    """Takes files and outputs only those that have been deleted."""
    def __init__(self):
        Operator.__init__(self)

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        """
        input_queue:
            A Queue of File(s).

        output_queue:
            A Queue of File(s) that have been deleted.
        """
        file = input_queue.get()
        while not isinstance(file, TerminateOperand):
            if not file.exists:
                output_queue.put(file)

            file = input_queue.get()

        # Put the TerminateOperand back so other consumers get it.
        input_queue.put(file)
