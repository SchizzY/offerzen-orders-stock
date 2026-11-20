import logging
import sys
from contextlib import suppress
from typing import TYPE_CHECKING

from asyncpg import PostgresError
from loguru import logger
from sqlalchemy.exc import StatementError

from app.config import settings

if TYPE_CHECKING:
    from loguru import Record

INTERCEPTED_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access", "sqlalchemy", "asyncpg")

# StatementError covers DBAPIError. Their messages, Postgres DETAIL lines and SQL text carry
# row values, so a record holding one keeps only the class and SQLSTATE.
DB_ERRORS = (StatementError, PostgresError)

# Loguru starts with its own stderr sink as handler 0.
_stderr_handler_id = 0


def _find_db_error(error: BaseException) -> BaseException | None:
    pending: list[BaseException | None] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, DB_ERRORS):
            return current
        pending += [current.__cause__, current.__context__]
        if isinstance(current, BaseExceptionGroup):
            pending += current.exceptions
    return None


def describe_error(error: BaseException) -> str:
    error = _find_db_error(error) or error
    sqlstate = getattr(getattr(error, "orig", error), "sqlstate", None)
    return f"error={type(error).__name__} sqlstate={sqlstate or '-'}"


def _scrub_db_errors(record: "Record") -> None:
    exception = record["exception"]
    if exception is None or exception.value is None or _find_db_error(exception.value) is None:
        return
    first_line = record["message"].partition("\n")[0]
    record["message"] = f"{first_line} {describe_error(exception.value)}"
    record["exception"] = None


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
        except Exception:
            # handleError would print record.args, which can hold customer data.
            logger.error(
                "unformattable log record from {} at {}:{}",
                record.name,
                record.pathname,
                record.lineno,
            )
            return
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 0
        while frame is not None and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, message)


def setup_logging() -> None:
    global _stderr_handler_id
    logger.configure(patcher=_scrub_db_errors)
    with suppress(ValueError):
        logger.remove(_stderr_handler_id)
    _stderr_handler_id = logger.add(
        sys.stderr,
        level=settings.log_level,
        serialize=settings.log_json,
        diagnose=False,
        backtrace=False,
    )
    logging.basicConfig(
        handlers=[InterceptHandler()], level=logger.level(settings.log_level).no, force=True
    )
    for name in INTERCEPTED_LOGGERS:
        stdlib_logger = logging.getLogger(name)
        stdlib_logger.handlers.clear()
        stdlib_logger.propagate = True
