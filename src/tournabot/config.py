from __future__ import annotations

from dataclasses import dataclass
from os import getenv


@dataclass(slots=True)
class Settings:
    telegram_bot_token: str
    team_affiliate_keywords: list[str]
    smoothcomp_events_url: str
    smoothcomp_events_fallback_url: str
    smoothcomp_timeout_seconds: float
    smoothcomp_default_country: str


    @classmethod
    def from_env(cls) -> "Settings":
        token = getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            raise ValueError("Missing TELEGRAM_BOT_TOKEN")

        affiliates_csv = getenv("TEAM_AFFILIATE_KEYWORDS", "")
        affiliates = [x.strip().lower() for x in affiliates_csv.split(",") if x.strip()]
        if not affiliates:
            raise ValueError("Set TEAM_AFFILIATE_KEYWORDS, comma-separated")

        events_url = getenv(
            "SMOOTHCOMP_EVENTS_URL",
            "https://smoothcomp.com/en/events/upcoming",
        ).strip()
        fallback_url = getenv(
            "SMOOTHCOMP_EVENTS_FALLBACK_URL",
            "https://compseek.net/events/smoothcomp",
        ).strip()

        country = getenv("SMOOTHCOMP_DEFAULT_COUNTRY", "Philippines").strip()

        timeout = float(getenv("SMOOTHCOMP_TIMEOUT_SECONDS", "20"))

        return cls(
            telegram_bot_token=token,
            team_affiliate_keywords=affiliates,
            smoothcomp_events_url=events_url,
            smoothcomp_events_fallback_url=fallback_url,
            smoothcomp_timeout_seconds=timeout,
            smoothcomp_default_country=country,
        )
