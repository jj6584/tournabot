from __future__ import annotations

from collections import defaultdict
from html import escape

from .models import CompetitorSchedule, Event


def format_events(events: list[Event]) -> str:
    if not events:
        return "No events found for your filter."

    lines = ["<b>Events found</b>", "Select one from the buttons below:"]
    for idx, event in enumerate(events, start=1):
        date = "TBD"
        if event.start_date and event.end_date:
            date = f"{event.start_date:%b %d, %Y} - {event.end_date:%b %d, %Y}"
        elif event.start_date:
            date = f"{event.start_date:%b %d, %Y}"

        loc = f"{event.location}, {event.country}".strip(", ")
        lines.append(
            f"{idx}. <b>{escape(event.name)}</b>\n"
            f"   Date: {escape(date)}\n"
            f"   Location: {escape(loc or 'TBD')}"
        )
    return "\n".join(lines)


def format_competitors(
    event: Event,
    competitor_rows: list[CompetitorSchedule],
    affiliate_keywords: list[str],
) -> str:
    if not competitor_rows:
        return (
            f"<b>{escape(event.name)}</b>\n"
            "No team competitors found for this event with your current affiliate filters."
        )

    lines = [
        f"<b>{escape(event.name)}</b>",
        "",
        f"Affiliate filter: <code>{escape(', '.join(affiliate_keywords))}</code>",
        "",
        "<b>Team competitors and schedules</b>",
    ]

    by_name: dict[str, list[CompetitorSchedule]] = defaultdict(list)
    for row in competitor_rows:
        by_name[row.competitor_name].append(row)

    for competitor_name in sorted(by_name.keys()):
        rows = by_name[competitor_name]
        academy = rows[0].academy or "TBD"
        lines.append(f"\n<b>{escape(competitor_name)}</b> ({escape(academy)})")
        for row in rows:
            lines.append(
                "- "
                f"Division: {escape(row.division or 'TBD')} | "
                f"Time: {escape(row.match_time)} | "
                f"Mat: {escape(row.mat)} | "
                f"Opponent: {escape(row.opponent)}"
            )

    lines.append("\n<b>Quick bracket view</b>")
    lines.extend(build_bracket_lines(competitor_rows))

    return "\n".join(lines)


def build_bracket_lines(competitors: list[CompetitorSchedule]) -> list[str]:
    by_division: dict[str, list[CompetitorSchedule]] = defaultdict(list)
    for row in competitors:
        by_division[row.division or "Unspecified division"].append(row)

    out: list[str] = []
    for division in sorted(by_division.keys()):
        out.append(f"\n{escape(division)}")
        for row in sorted(by_division[division], key=lambda x: x.competitor_name):
            out.append(
                f"{escape(row.competitor_name)} vs {escape(row.opponent)} "
                f"(Mat {escape(row.mat)}, {escape(row.match_time)})"
            )
    return out
