from __future__ import annotations

import logging

from dotenv import load_dotenv

from .bot import TournaBot
from .config import Settings


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    load_dotenv()
    settings = Settings.from_env()
    bot = TournaBot(settings)
    bot.run()


if __name__ == "__main__":
    main()
