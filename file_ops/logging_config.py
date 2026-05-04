import logging


class VerbosityFilter(logging.Filter):
    def __init__(self, verbosity_level: int, name: str = "") -> None:
        super().__init__(name)
        self.verbosity_level = verbosity_level

    def filter(self, record: logging.LogRecord) -> bool:
        record_verbosity = getattr(record, "verbosity", 0)
        return record_verbosity <= self.verbosity_level


def configure_logging(log_level: str = "INFO", verbosity: int = 1) -> None:
    handler = logging.StreamHandler()
    handler.addFilter(VerbosityFilter(verbosity_level=verbosity))

    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.getLevelNamesMapping()[log_level],
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[handler],
    )


def verbosity_extra(level: int) -> dict:
    return {"verbosity": level}
