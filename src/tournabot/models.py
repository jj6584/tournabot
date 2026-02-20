from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(slots=True)
class Event:
    id: str
    name: str
    url: str
    location: str = ""
    country: str = ""
    start_date: datetime | None = None
    end_date: datetime | None = None


@dataclass(slots=True)
class CompetitorSchedule:
    competitor_name: str
    academy: str
    division: str = ""
    bracket: str = ""
    opponent: str = "TBD"
    match_time: str = "TBD"
    mat: str = "TBD"
    source_url: str = ""
    tags: list[str] = field(default_factory=list)
