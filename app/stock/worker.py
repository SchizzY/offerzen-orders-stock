"""Stock worker process: `python -m app.stock.worker`."""

import os
import signal


def _exit(signum: int, frame: object) -> None:
    os._exit(0)


if __name__ == "__main__":
    # Installed before the slow imports below. Until the consumer loop takes the signals over,
    # nothing is in flight, so a stop signal just exits.
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, _exit)

    from app.stock.consumer import main

    main()
