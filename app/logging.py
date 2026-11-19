import logging
import sys

from loguru import logger

from app.config import settings

INTERCEPTED_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access", "sqlalchemy", "asyncpg")

_stderr_handler_id: int | None = None


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 0
        while frame is not None and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging() -> None:
    global _stderr_handler_id
    level = settings.log_level.upper()
    # First call drops loguru's default sink; later calls replace only ours, so sinks
    # added by others (tests, tooling) survive.
    logger.remove(_stderr_handler_id)
    _stderr_handler_id = logger.add(
        sys.stderr,
        level=level,
        serialize=settings.log_json,
        diagnose=False,
        backtrace=False,
    )
    logging.basicConfig(handlers=[InterceptHandler()], level=logger.level(level).no, force=True)
    for name in INTERCEPTED_LOGGERS:
        stdlib_logger = logging.getLogger(name)
        stdlib_logger.handlers.clear()
        stdlib_logger.propagate = True
