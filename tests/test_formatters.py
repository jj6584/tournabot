from datetime import datetime

from tournabot.formatters import format_competitors
from tournabot.models import CompetitorSchedule, Event


def test_format_competitors_includes_bracket_view() -> None:
    event = Event(
        id="1",
        name="Sample Open",
        url="https://smoothcomp.com/en/event/1",
        start_date=datetime(2026, 4, 2),
        end_date=datetime(2026, 4, 3),
    )
    rows = [
        CompetitorSchedule(
            competitor_name="Alice Santos",
            academy="Alpha Academy",
            division="Adult -70kg",
            opponent="Jane Cruz",
            match_time="10:30 AM",
            mat="Mat 2",
        )
    ]

    text = format_competitors(event, rows, ["alpha"])

    assert "Quick bracket view" in text
    assert "Alice Santos vs Jane Cruz" in text
