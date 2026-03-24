import logging

from logger import Logger

log = logging.getLogger(__name__)


def main() -> None:
    Logger("main")
    log.info("Hello from seasonality!")


if __name__ == "__main__":
    main()
