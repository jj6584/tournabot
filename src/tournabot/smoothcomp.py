from __future__ import annotations

import logging
import re
from html import unescape
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from bs4.element import Tag

from .models import CompetitorSchedule, Event

_EVENT_LINK_PATTERN = re.compile(r"/(?:[a-z]{2}/)?event/\d+", re.IGNORECASE)
_EVENT_URL_PATTERN = re.compile(
    r"/(?:[a-z]{2}/)?event/\d+(?:/[A-Za-z0-9\-_]+)?",
    re.IGNORECASE,
)
_PROFILE_LINK_PATTERN = re.compile(r"/(?:[a-z]{2}/)?profile/\d+", re.IGNORECASE)
LOGGER = logging.getLogger(__name__)
_GENERIC_EVENT_NAMES = {
    "smoothcomp",
    "event",
    "details",
    "read more",
    "open",
    "view",
}
_MONTHS_PATTERN = (
    r"January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)


class SmoothcompClient:
    def __init__(
        self,
        base_events_url: str,
        fallback_events_url: str = "https://compseek.net/events/smoothcomp",
        timeout_seconds: float = 20.0,
    ) -> None:
        self.base_events_url = base_events_url
        self.fallback_events_url = fallback_events_url
        self.timeout_seconds = timeout_seconds

    async def fetch_events(self, *, year: int, country: str) -> list[Event]:
        today = date.today()
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(self.base_events_url)
            response.raise_for_status()

        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        candidates = self._collect_event_candidates(soup=soup, html=html)

        strict_country: list[Event] = []
        strict_country_unknown_year: list[Event] = []
        strict_country_other_year: list[Event] = []
        relaxed: list[Event] = []
        relaxed_unknown_year: list[Event] = []
        relaxed_other_year: list[Event] = []
        for url, payload in candidates.items():
            raw_name = str(payload.get("name", "")).strip()
            name = raw_name or self._name_from_url(url)
            context_blob = " ".join(payload.get("contexts", []))
            parsed_dates = self._extract_dates_from_text(context_blob)
            event_year = parsed_dates[0].year if parsed_dates else None
            if parsed_dates and parsed_dates[0].date() < today:
                continue

            event = Event(
                id=self._event_id_from_url(url),
                name=name,
                url=url,
                location=self._extract_location(context_blob),
                country=country if self._country_in_text(context_blob, country) else "",
                start_date=parsed_dates[0] if parsed_dates else None,
                end_date=parsed_dates[1] if parsed_dates and len(parsed_dates) > 1 else None,
            )

            in_country = self._country_in_text(context_blob, country)
            if in_country and event_year == year:
                strict_country.append(event)
            elif in_country and event_year is None:
                strict_country_unknown_year.append(event)
            elif in_country:
                strict_country_other_year.append(event)
            elif event_year == year:
                relaxed.append(event)
            elif event_year is None:
                relaxed_unknown_year.append(event)
            else:
                relaxed_other_year.append(event)

        def _sort_key(e: Event) -> tuple[datetime, str]:
            max_date = datetime.max
            return (e.start_date or max_date, e.name.lower())

        all_groups = [
            strict_country,
            strict_country_unknown_year,
            strict_country_other_year,
            relaxed,
            relaxed_unknown_year,
            relaxed_other_year,
        ]
        for group in all_groups:
            group.sort(key=_sort_key)

        if strict_country or strict_country_unknown_year:
            picked = strict_country + strict_country_unknown_year
            LOGGER.info(
                "Events: %d country+year, %d country+unknown-year",
                len(strict_country),
                len(strict_country_unknown_year),
            )
            return picked
        if strict_country_other_year:
            LOGGER.info(
                "Events: no country+year matches, falling back to %d country events from other years",
                len(strict_country_other_year),
            )
            return strict_country_other_year
        if relaxed or relaxed_unknown_year:
            picked = relaxed + relaxed_unknown_year
            LOGGER.info(
                "Events: no country-tagged matches, falling back to %d non-country events",
                len(picked),
            )
            return picked

        LOGGER.info("Events: zero candidates found from Smoothcomp page, trying fallback source")
        return await self._fetch_events_from_fallback(year=year, country=country, today=today)

    @staticmethod
    def event_url_from_id(event_id: str) -> str:
        return f"https://smoothcomp.com/en/event/{event_id}"

    async def fetch_event_by_id(self, event_id: str, default_country: str) -> Event | None:
        urls = await self._candidate_event_urls_for_id(event_id)
        for url in urls:
            event = await self.fetch_event_by_url(url, default_country)
            if event is not None and event.id == event_id:
                return event
        return None

    async def fetch_event_by_url(self, event_url: str, default_country: str) -> Event | None:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            try:
                response = await client.get(event_url)
                response.raise_for_status()
            except httpx.HTTPError:
                return None

        final_url = self._canonical_event_url(str(response.url))
        soup = BeautifulSoup(response.text, "html.parser")
        title = self._extract_event_name_from_page(soup, response.text, final_url)
        blob = " ".join(soup.stripped_strings)
        dates = self._extract_dates_from_text(blob)
        country = default_country if self._country_in_text(blob, default_country) else ""
        return Event(
            id=self._event_id_from_url(final_url),
            name=title,
            url=final_url,
            location=self._extract_location(blob),
            country=country,
            start_date=dates[0] if dates else None,
            end_date=dates[1] if len(dates) > 1 else None,
        )

    async def search_events_by_name(
        self,
        *,
        name_query: str,
        year: int,
        country: str,
        limit: int = 10,
    ) -> list[Event]:
        primary = await self.fetch_events(year=year, country=country)
        fallback_broad = await self._fetch_events_from_fallback(year=year, country=country, today=None)
        merged: dict[str, Event] = {e.url: e for e in primary}
        for event in fallback_broad:
            merged.setdefault(event.url, event)

        events = list(merged.values())
        query = self._normalize_name(name_query)
        if not query:
            return events[:limit]

        query_tokens = [t for t in re.split(r"[^a-z0-9]+", query) if t]
        scored: list[tuple[int, Event]] = []
        for event in events:
            name_raw = event.name
            name = self._normalize_name(name_raw)
            score = 0
            if name == query:
                score += 1000
            elif query in name:
                score += 600
            elif name and name in query:
                score += 250
            for token in query_tokens:
                if token in name:
                    score += 30
            if str(year) in name_raw:
                score += 40
            if score > 0:
                scored.append((score, event))

        scored.sort(key=lambda row: (-row[0], row[1].start_date or datetime.max, row[1].name.lower()))
        return [event for _, event in scored[:limit]]

    async def debug_event_discovery(self, *, year: int, country: str) -> str:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(self.base_events_url)
            response.raise_for_status()

        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        anchor_hits = 0
        for link in soup.find_all("a", href=True):
            if _EVENT_LINK_PATTERN.search(link["href"]):
                anchor_hits += 1
        regex_hits = len(_EVENT_URL_PATTERN.findall(html))
        candidates = self._collect_event_candidates(soup=soup, html=html)

        country_hits = 0
        year_hits = 0
        sample_urls: list[str] = []
        for url, payload in candidates.items():
            context_blob = " ".join(payload.get("contexts", []))
            if self._country_in_text(context_blob, country):
                country_hits += 1
            parsed_dates = self._extract_dates_from_text(context_blob)
            if parsed_dates and parsed_dates[0].year == year:
                year_hits += 1
            if len(sample_urls) < 10:
                sample_urls.append(url)

        lines = [
            f"Base URL: {self.base_events_url}",
            f"Anchor matches: {anchor_hits}",
            f"Regex matches: {regex_hits}",
            f"Unique candidates: {len(candidates)}",
            f"Country hits ({country}): {country_hits}",
            f"Year hits ({year}): {year_hits}",
            "Sample candidate URLs:",
        ]
        if sample_urls:
            lines.extend(f"- {url}" for url in sample_urls)
        else:
            lines.append("- none")

        fallback = await self._fetch_events_from_fallback(year=year, country=country)
        lines.append(f"Fallback source ({self.fallback_events_url}) events: {len(fallback)}")
        for event in fallback[:5]:
            lines.append(f"- {event.name} | {event.url}")
        return "\n".join(lines)

    async def _fetch_events_from_fallback(self, *, year: int, country: str, today: date | None = None) -> list[Event]:
        if today is None:
            today = date.today()
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(self.fallback_events_url)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        all_found: dict[str, Event] = {}
        year_found: dict[str, Event] = {}
        country_found: dict[str, Event] = {}
        country_year_found: dict[str, Event] = {}
        strict_found: dict[str, Event] = {}

        for link in soup.find_all("a", href=True):
            href = str(link["href"]).strip()
            url = self._normalize_event_url(href)
            if not url:
                continue

            row_text = " ".join(link.parent.stripped_strings)
            tr = link.find_parent("tr")
            if tr is not None:
                row_text = " ".join(tr.stripped_strings)
            lower = row_text.lower()
            in_country = country.lower() in lower or self._country_in_text(lower, country)

            dates = self._extract_dates_from_text(row_text)

            name = self._extract_event_name_from_link(
                link=link,
                row_text=row_text,
                url=url,
            )
            event = Event(
                id=self._event_id_from_url(url),
                name=name,
                url=url,
                location=self._extract_location(row_text),
                country=country if in_country else "",
                start_date=dates[0] if dates else None,
                end_date=dates[1] if len(dates) > 1 else None,
            )
            all_found[url] = event
            if dates and dates[0].year == year:
                year_found[url] = event
            if in_country:
                country_found[url] = event
                if dates and dates[0].year == year:
                    country_year_found[url] = event
                if dates:
                    if dates[0].year != year:
                        continue
                    if dates[0].date() < today:
                        continue
                strict_found[url] = event

        chosen: dict[str, Event]
        if today is None:
            if country_year_found:
                chosen = country_year_found
            elif year_found:
                chosen = year_found
            elif country_found:
                chosen = country_found
            else:
                chosen = all_found
        elif strict_found:
            chosen = strict_found
        elif country_year_found:
            LOGGER.info("Fallback: no upcoming matches; returning country+year results")
            chosen = country_year_found
        elif year_found:
            LOGGER.info("Fallback: no country-tagged year matches; returning year-only results")
            chosen = year_found
        elif country_found:
            LOGGER.info("Fallback: no year-tagged results; returning country-only results as last resort")
            chosen = country_found
        else:
            LOGGER.info("Fallback: no country matches; returning all fallback results")
            chosen = all_found

        events = list(chosen.values())
        events.sort(key=lambda e: (e.start_date or datetime.max, e.name.lower()))
        return events

    async def fetch_competitors_for_event(
        self,
        event: Event,
        affiliate_keywords: list[str],
    ) -> list[CompetitorSchedule]:
        pages_to_try = self._event_pages_to_try(event.url)

        html_pages: list[tuple[str, str]] = []
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            for url in pages_to_try:
                try:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        html_pages.append((url, resp.text))
                except httpx.HTTPError:
                    continue

        competitors: list[CompetitorSchedule] = []
        seen: set[tuple[str, str, str, str]] = set()

        for source_url, html in html_pages:
            soup = BeautifulSoup(html, "html.parser")
            competitors.extend(
                self._parse_schedule_brackets_page(
                    soup=soup,
                    affiliate_keywords=affiliate_keywords,
                    source_url=source_url,
                    seen=seen,
                )
            )
            competitors.extend(
                self._parse_participant_profile_links(
                    soup=soup,
                    affiliate_keywords=affiliate_keywords,
                    source_url=source_url,
                    seen=seen,
                )
            )
            competitors.extend(
                self._parse_competitor_tables(
                    soup=soup,
                    affiliate_keywords=affiliate_keywords,
                    source_url=source_url,
                    seen=seen,
                )
            )
            competitors.extend(
                self._parse_competitor_scripts(
                    soup=soup,
                    affiliate_keywords=affiliate_keywords,
                    source_url=source_url,
                    seen=seen,
                )
            )
            competitors.extend(
                self._parse_competitor_blocks(
                    soup=soup,
                    affiliate_keywords=affiliate_keywords,
                    source_url=source_url,
                    seen=seen,
                )
            )

        return competitors

    def _parse_participant_profile_links(
        self,
        *,
        soup: BeautifulSoup,
        affiliate_keywords: list[str],
        source_url: str,
        seen: set[tuple[str, str, str, str]],
    ) -> list[CompetitorSchedule]:
        rows: list[CompetitorSchedule] = []
        for link in soup.find_all("a", href=True):
            href = str(link.get("href", ""))
            if not _PROFILE_LINK_PATTERN.search(href):
                continue

            name = " ".join(link.stripped_strings).strip()
            name = re.sub(r"\s+", " ", name)
            name = name.replace("...", "").strip()
            if not self._is_plausible_person_name(name):
                continue

            card = link.find_parent(["article", "li", "div"])
            if card is None:
                continue
            card_text = " ".join(card.stripped_strings)
            if len(card_text) < 8:
                continue
            if affiliate_keywords and not self._text_matches_affiliate(card_text, affiliate_keywords):
                continue

            affiliate = self._extract_affiliate_from_text(card_text, affiliate_keywords) or "Unknown"
            division = self._find_nearest_division_heading(card) or self._guess_division([card_text]) or "TBD"
            match_time = self._guess_time([card_text]) or "TBD"
            mat = self._guess_mat([card_text]) or "TBD"
            opponent = self._extract_opponent([card_text], name)

            key = (name, division, match_time, mat)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                CompetitorSchedule(
                    competitor_name=name,
                    academy=affiliate,
                    division=division,
                    opponent=opponent,
                    match_time=match_time,
                    mat=mat,
                    source_url=source_url,
                )
            )
        return rows

    @staticmethod
    def _find_nearest_division_heading(node: Tag) -> str:
        heading = node.find_previous(["h1", "h2", "h3", "h4"])
        if not heading:
            return ""
        text = " ".join(heading.stripped_strings).strip()
        if "/" in text and len(text) >= 8:
            return text
        return ""

    async def detect_affiliates_for_event(self, event: Event, limit: int = 12) -> list[str]:
        rows = await self.fetch_competitors_for_event(event, affiliate_keywords=[])
        counts: dict[str, int] = {}
        for row in rows:
            academy = (row.academy or "").strip()
            if not academy:
                continue
            low = academy.lower()
            if low in {"tbd", "team match", "unknown"}:
                continue
            if not self._is_likely_affiliate_label(academy):
                continue
            counts[academy] = counts.get(academy, 0) + 1
        ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))
        return [name for name, _ in ordered[:limit]]

    async def detect_people_for_event(self, event: Event, limit: int = 15) -> list[tuple[str, str]]:
        rows = await self.fetch_competitors_for_event(event, affiliate_keywords=[])
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for row in rows:
            name = (row.competitor_name or "").strip()
            if not self._is_plausible_person_name(name):
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            division = (row.division or "TBD").strip()
            out.append((name, division))
            if len(out) >= limit:
                break
        return out

    def _event_pages_to_try(self, event_url: str) -> list[str]:
        base = self._canonical_event_url(event_url).rstrip("/")
        variants = {base}
        variants.update(
            {
                f"{base}/participants",
                f"{base}/participants?page=1",
                f"{base}/participants?page=2",
                f"{base}/participants?page=3",
                f"{base}/participants?page=4",
                f"{base}/schedule/brackets",
                f"{base}/schedule",
                f"{base}/bracket",
                f"{base}/brackets",
                f"{base}/matches",
                f"{base}/registrations",
            }
        )
        if "/en/event/" in base:
            alt = base.replace("/en/event/", "/event/")
            variants.update(
                {
                    alt,
                    f"{alt}/participants",
                    f"{alt}/schedule/brackets",
                    f"{alt}/schedule",
                    f"{alt}/bracket",
                    f"{alt}/matches",
                }
            )
        elif "/event/" in base:
            alt = base.replace("/event/", "/en/event/")
            variants.update(
                {
                    alt,
                    f"{alt}/participants",
                    f"{alt}/schedule/brackets",
                    f"{alt}/schedule",
                    f"{alt}/bracket",
                    f"{alt}/matches",
                }
            )
        ordered = []
        if f"{base}/schedule/brackets" in variants:
            ordered.append(f"{base}/schedule/brackets")
        if f"{base}/participants" in variants:
            ordered.append(f"{base}/participants")
        for url in sorted(variants):
            if url not in ordered:
                ordered.append(url)
        return ordered

    def _parse_schedule_brackets_page(
        self,
        *,
        soup: BeautifulSoup,
        affiliate_keywords: list[str],
        source_url: str,
        seen: set[tuple[str, str, str, str]],
    ) -> list[CompetitorSchedule]:
        rows: list[CompetitorSchedule] = []

        # Parse list/table rows where time and group are visible.
        for tr in soup.find_all("tr"):
            row_text = " ".join(tr.stripped_strings)
            if not row_text:
                continue
            if "/" not in row_text and "bracket" not in row_text.lower():
                continue

            division = self._extract_divisionish_text(row_text) or self._guess_division([row_text]) or "TBD"
            match_time = self._guess_time([row_text]) or "TBD"
            mat = self._guess_mat([row_text]) or "TBD"

            names = self._extract_people_from_text(row_text)
            for name in names:
                if not self._is_plausible_person_name(name):
                    continue
                if affiliate_keywords and not self._text_matches_affiliate(row_text, affiliate_keywords):
                    continue
                affiliate = self._extract_affiliate_from_text(row_text, affiliate_keywords) or "Unknown"
                key = (name, division, match_time, mat)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    CompetitorSchedule(
                        competitor_name=name,
                        academy=affiliate,
                        division=division,
                        opponent="TBD",
                        match_time=match_time,
                        mat=mat,
                        source_url=source_url,
                    )
                )

        # Parse bracket modal payloads usually embedded in scripts.
        for script in soup.find_all("script"):
            text = script.string or script.get_text(" ", strip=False)
            if not text:
                continue
            lower = text.lower()
            if "semifinal" not in lower and "bracket" not in lower and "final" not in lower:
                continue

            division = self._extract_divisionish_text(text) or "TBD"
            match_time = self._guess_time([text]) or "TBD"
            mat = self._guess_mat([text]) or "TBD"
            pairs = re.findall(
                r"([A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){1,4})\s*(?:vs\.?|versus)\s*([A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){1,4})",
                text,
                re.IGNORECASE,
            )
            for left, right in pairs:
                left_name = left.strip()
                right_name = right.strip()
                for name, opponent in ((left_name, right_name), (right_name, left_name)):
                    if not self._is_plausible_person_name(name):
                        continue
                    if affiliate_keywords and not self._text_matches_affiliate(text, affiliate_keywords):
                        continue
                    affiliate = self._extract_affiliate_from_text(text, affiliate_keywords) or "Unknown"
                    key = (name, division, match_time, mat)
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(
                        CompetitorSchedule(
                            competitor_name=name,
                            academy=affiliate,
                            division=division,
                            opponent=opponent,
                            match_time=match_time,
                            mat=mat,
                            source_url=source_url,
                        )
                    )
        return rows

    def _parse_competitor_tables(
        self,
        *,
        soup: BeautifulSoup,
        affiliate_keywords: list[str],
        source_url: str,
        seen: set[tuple[str, str, str, str]],
    ) -> list[CompetitorSchedule]:
        rows: list[CompetitorSchedule] = []
        for tr in soup.find_all("tr"):
            cells = [" ".join(td.stripped_strings) for td in tr.find_all(["td", "th"])]
            if len(cells) < 2:
                continue

            joined = " | ".join(cells)
            lower_joined = joined.lower()
            if any(token in lower_joined for token in ["name", "academy", "team", "division"]) and len(cells) < 4:
                continue

            affiliate = self._first_match(cells, ["academy", "affiliate", "team", "club"]) or self._guess_affiliate(cells)
            if not affiliate:
                continue
            if affiliate_keywords and not self._text_matches_affiliate(affiliate, affiliate_keywords):
                continue

            competitor_name = self._guess_name(cells)
            if not competitor_name:
                continue

            division = self._first_match(cells, ["division", "category", "weight", "belt"]) or self._guess_division(cells)
            match_time = self._first_match(cells, ["time", "start", "schedule"]) or self._guess_time(cells)
            mat = self._first_match(cells, ["mat", "ring", "area"]) or self._guess_mat(cells)
            opponent = self._extract_opponent(cells, competitor_name)
            bracket = self._first_match(cells, ["bracket", "pool"]) or ""

            key = (competitor_name, division or "", match_time or "", mat or "")
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                CompetitorSchedule(
                    competitor_name=competitor_name,
                    academy=affiliate,
                    division=division or "TBD",
                    bracket=bracket,
                    opponent=opponent,
                    match_time=match_time or "TBD",
                    mat=mat or "TBD",
                    source_url=source_url,
                )
            )
        return rows

    def _parse_competitor_scripts(
        self,
        *,
        soup: BeautifulSoup,
        affiliate_keywords: list[str],
        source_url: str,
        seen: set[tuple[str, str, str, str]],
    ) -> list[CompetitorSchedule]:
        rows: list[CompetitorSchedule] = []
        lowered_keywords = [k.lower() for k in affiliate_keywords]
        for script in soup.find_all("script"):
            text = script.string or script.get_text(" ", strip=False)
            if not text:
                continue
            lower = text.lower()
            if lowered_keywords and not any(self._text_matches_affiliate(lower, [k]) for k in lowered_keywords):
                continue

            for obj in re.findall(r"\{[^{}]{20,2000}\}", text):
                obj_low = obj.lower()
                if lowered_keywords and not self._text_matches_affiliate(obj_low, lowered_keywords):
                    continue
                affiliate = self._json_field(obj, ["academy", "affiliate", "team", "club"])
                if not affiliate and lowered_keywords:
                    affiliate = self._extract_affiliate_from_text(obj, affiliate_keywords)
                if not affiliate:
                    affiliate = "Unknown"
                if lowered_keywords and not self._text_matches_affiliate(affiliate, affiliate_keywords):
                    continue
                competitor_name = self._json_field(obj, ["name", "competitor_name", "athlete_name", "fighter_name"])
                if not competitor_name:
                    continue

                division = self._json_field(obj, ["division", "category", "weight_class", "weight"]) or "TBD"
                mat = self._json_field(obj, ["mat", "ring", "area"]) or self._guess_mat([obj]) or "TBD"
                match_time = self._json_field(obj, ["time", "start_time", "schedule_time"]) or self._guess_time([obj]) or "TBD"
                opponent = self._json_field(obj, ["opponent", "versus", "vs", "enemy"]) or "TBD"

                key = (competitor_name, division, match_time, mat)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    CompetitorSchedule(
                        competitor_name=unescape(competitor_name),
                        academy=unescape(affiliate),
                        division=unescape(division),
                        opponent=unescape(opponent),
                        match_time=unescape(match_time),
                        mat=unescape(mat),
                        source_url=source_url,
                    )
                )

            rows.extend(
                self._parse_competitor_script_windows(
                    script_text=text,
                    affiliate_keywords=affiliate_keywords,
                    source_url=source_url,
                    seen=seen,
                )
            )
        return rows

    def _parse_competitor_script_windows(
        self,
        *,
        script_text: str,
        affiliate_keywords: list[str],
        source_url: str,
        seen: set[tuple[str, str, str, str]],
    ) -> list[CompetitorSchedule]:
        out: list[CompetitorSchedule] = []
        if not affiliate_keywords:
            return out
        lower = script_text.lower()
        for keyword in affiliate_keywords:
            kw = keyword.lower().strip()
            if not kw:
                continue
            start = 0
            while True:
                idx = lower.find(kw, start)
                if idx == -1:
                    break
                win_start = max(0, idx - 500)
                win_end = min(len(script_text), idx + 500)
                snippet = script_text[win_start:win_end]
                affiliate = self._extract_affiliate_from_text(snippet, affiliate_keywords) or keyword
                competitor_name = (
                    self._json_field(snippet, ["name", "competitor_name", "athlete_name", "fighter_name"])
                    or self._extract_person_name(snippet)
                )
                if competitor_name and self._is_plausible_person_name(competitor_name):
                    division = self._json_field(snippet, ["division", "category", "weight_class", "weight"]) or "TBD"
                    mat = self._json_field(snippet, ["mat", "ring", "area"]) or self._guess_mat([snippet]) or "TBD"
                    match_time = self._json_field(snippet, ["time", "start_time", "schedule_time"]) or self._guess_time([snippet]) or "TBD"
                    opponent = self._json_field(snippet, ["opponent", "versus", "vs", "enemy"]) or "TBD"
                    key = (competitor_name, division, match_time, mat)
                    if key not in seen:
                        seen.add(key)
                        out.append(
                            CompetitorSchedule(
                                competitor_name=unescape(competitor_name),
                                academy=unescape(affiliate),
                                division=unescape(division),
                                opponent=unescape(opponent),
                                match_time=unescape(match_time),
                                mat=unescape(mat),
                                source_url=source_url,
                            )
                        )
                start = idx + len(kw)
        return out

    def _parse_competitor_blocks(
        self,
        *,
        soup: BeautifulSoup,
        affiliate_keywords: list[str],
        source_url: str,
        seen: set[tuple[str, str, str, str]],
    ) -> list[CompetitorSchedule]:
        rows: list[CompetitorSchedule] = []
        selectors = [
            "[class*='participant']",
            "[class*='competitor']",
            "[class*='athlete']",
            "[class*='fighter']",
            "[class*='entry']",
            "[class*='card']",
            "[class*='row']",
            "li",
            "article",
        ]
        scanned = 0
        for sel in selectors:
            for node in soup.select(sel):
                scanned += 1
                if scanned > 2200:
                    return rows
                text = " ".join(node.stripped_strings)
                if len(text) < 10:
                    continue
                if affiliate_keywords and not self._text_matches_affiliate(text, affiliate_keywords):
                    continue
                competitor_name = self._extract_person_name(text)
                if not competitor_name or not self._is_plausible_person_name(competitor_name):
                    continue
                affiliate = self._extract_affiliate_from_text(text, affiliate_keywords) or "Team match"
                division = self._guess_division([text]) or "TBD"
                mat = self._guess_mat([text]) or "TBD"
                match_time = self._guess_time([text]) or "TBD"
                opponent = self._extract_opponent([text], competitor_name)
                key = (competitor_name, division, match_time, mat)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    CompetitorSchedule(
                        competitor_name=competitor_name,
                        academy=affiliate,
                        division=division,
                        opponent=opponent,
                        match_time=match_time,
                        mat=mat,
                        source_url=source_url,
                    )
                )
        return rows

    @staticmethod
    def _event_id_from_url(url: str) -> str:
        match = re.search(r"/event/(\d+)", url)
        return match.group(1) if match else url

    @staticmethod
    def _extract_dates_from_text(text: str) -> list[datetime]:
        patterns = [
            r"(\d{4}-\d{2}-\d{2})",
            r"([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})",
        ]
        out: list[datetime] = []
        for pattern in patterns:
            for m in re.findall(pattern, text):
                for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"):
                    try:
                        out.append(datetime.strptime(m, fmt))
                        break
                    except ValueError:
                        continue
        out.sort()
        return out

    @staticmethod
    def _extract_location(text: str) -> str:
        for token in ["city", "venue", "location", "philippines"]:
            if token in text:
                return text[:120]
        return ""

    @staticmethod
    def _country_in_text(text: str, country: str) -> bool:
        lowered = text.lower()
        country_l = country.lower()
        if country_l in lowered:
            return True
        if country_l == "philippines":
            return bool(
                re.search(r"\bphilippines\b", lowered)
                or re.search(r"\bphl\b", lowered)
                or re.search(r"\bph\b", lowered)
            )
        return False

    def _collect_event_candidates(
        self,
        *,
        soup: BeautifulSoup,
        html: str,
    ) -> dict[str, dict[str, Any]]:
        candidates: dict[str, dict[str, Any]] = {}

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not _EVENT_LINK_PATTERN.search(href):
                continue
            url = self._normalize_event_url(href)
            if not url:
                continue
            context = " ".join(link.parent.stripped_strings).strip()
            name = " ".join(link.stripped_strings).strip()
            if self._is_generic_event_name(name):
                name = self._extract_event_name_from_link(
                    link=link,
                    row_text=context,
                    url=url,
                )
            record = candidates.setdefault(url, {"name": "", "contexts": []})
            if name and not record["name"]:
                record["name"] = name
            if context:
                record["contexts"].append(context)

        for match in _EVENT_URL_PATTERN.finditer(html):
            raw_href = match.group(0)
            url = self._normalize_event_url(raw_href)
            if not url:
                continue
            start = max(0, match.start() - 260)
            end = min(len(html), match.end() + 260)
            snippet = html[start:end]
            context = re.sub(r"\s+", " ", snippet).strip()
            record = candidates.setdefault(url, {"name": "", "contexts": []})
            record["contexts"].append(context)
            if not record["name"]:
                extracted = self._extract_name_from_context(context, url)
                if extracted:
                    record["name"] = extracted
        return candidates

    @staticmethod
    def _normalize_event_url(href: str) -> str:
        if not _EVENT_LINK_PATTERN.search(href):
            return ""
        return urljoin("https://smoothcomp.com", href.split("?", 1)[0])

    @staticmethod
    def _extract_name_from_context(context: str, url: str) -> str:
        json_name = re.search(r'"name"\s*:\s*"([^"]{4,120})"', context)
        if json_name:
            name = json_name.group(1).strip()
            if not SmoothcompClient._is_generic_event_name(name):
                return name
        title_name = re.search(r'title="([^"]{4,120})"', context)
        if title_name:
            name = title_name.group(1).strip()
            if not SmoothcompClient._is_generic_event_name(name):
                return name
        text_name = SmoothcompClient._extract_name_from_text_blob(context)
        if text_name:
            return text_name
        return SmoothcompClient._name_from_url(url)

    async def _candidate_event_urls_for_id(self, event_id: str) -> list[str]:
        candidates: dict[str, None] = {
            f"https://smoothcomp.com/en/event/{event_id}": None,
            f"https://smoothcomp.com/event/{event_id}": None,
        }
        pages = [self.base_events_url, self.fallback_events_url]
        pattern_abs = re.compile(
            rf"https?://[A-Za-z0-9.-]*smoothcomp\\.com/[A-Za-z0-9/_\\-]*/?event/{re.escape(event_id)}(?:/[A-Za-z0-9_\\-]+)?",
            re.IGNORECASE,
        )
        pattern_rel = re.compile(
            rf"/(?:[a-z]{{2}}/)?event/{re.escape(event_id)}(?:/[A-Za-z0-9_\\-]+)?",
            re.IGNORECASE,
        )
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            for page in pages:
                try:
                    resp = await client.get(page)
                    if resp.status_code != 200:
                        continue
                except httpx.HTTPError:
                    continue
                html = resp.text
                for m in pattern_abs.findall(html):
                    candidates[m.split("?", 1)[0]] = None
                for m in pattern_rel.findall(html):
                    candidates[urljoin(str(resp.url), m.split("?", 1)[0])] = None
        return list(candidates.keys())

    @staticmethod
    def _normalize_name(text: str) -> str:
        text = re.sub(r"\s+", " ", text).strip().lower()
        return re.sub(r"[^a-z0-9 ]+", "", text)

    @staticmethod
    def _json_field(obj_text: str, keys: list[str]) -> str:
        for key in keys:
            m = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]+)"', obj_text, re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return ""

    @staticmethod
    def _text_matches_affiliate(text: str, affiliate_keywords: list[str]) -> bool:
        hay = re.sub(r"\s+", " ", text).strip().lower()
        if not hay:
            return False
        if not affiliate_keywords:
            return True
        for keyword in affiliate_keywords:
            kw = keyword.lower().strip()
            if not kw:
                continue
            if kw in hay:
                return True
            tokens = [t for t in re.split(r"[^a-z0-9]+", kw) if len(t) >= 3]
            if not tokens:
                continue
            hits = sum(1 for token in tokens if token in hay)
            if hits >= 2 or (hits >= 1 and len(tokens) == 1):
                return True
        return False

    @staticmethod
    def _extract_affiliate_from_text(text: str, affiliate_keywords: list[str]) -> str:
        for keyword in affiliate_keywords:
            if keyword.lower() in text.lower():
                return keyword
        lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
        if len(lines) <= 1:
            parts = [p.strip() for p in re.split(r"\s{2,}|\|", text) if p.strip()]
            if parts:
                lines = parts
        for line in lines:
            if re.search(r"[A-Za-z].*(?:Jiu|Jitsu|BJJ|Team|Academy|Atos|DeBlass|TDBJJ)", line, re.IGNORECASE):
                return line[:120]
        for line in lines:
            if any(marker in line.lower() for marker in ["academy", "affiliate", "team", "club"]):
                return line[:120]
        if lines:
            return lines[-1][:120]
        return ""

    @staticmethod
    def _is_likely_affiliate_label(text: str) -> bool:
        t = re.sub(r"\s+", " ", text).strip()
        lower = t.lower()
        if len(t) < 3 or len(t) > 120:
            return False
        if re.search(r"\b\d{1,2}:\d{2}\s*(?:am|pm)\b", lower):
            return False
        if "/" in t and any(x in lower for x in ["kg", "adult", "master", "female", "male", "white", "blue", "brown", "black"]):
            return False
        if any(x in lower for x in ["participants", "ranking", "membership", "home events", "approved registrations"]):
            return False
        return True

    @staticmethod
    def _extract_person_name(text: str) -> str:
        # Try explicit key-value style first.
        kv = re.search(
            r"(?:name|athlete|competitor|fighter)\s*[:\"'= ]+\s*([A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){1,4})",
            text,
            re.IGNORECASE,
        )
        if kv:
            return kv.group(1).strip()
        # Fallback: first plausible title-cased multi-word string.
        generic = re.search(r"\b([A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){1,4})\b", text)
        return generic.group(1).strip() if generic else ""

    @staticmethod
    def _is_plausible_person_name(name: str) -> bool:
        n = name.strip()
        if len(n) < 5:
            return False
        lower = n.lower()
        blocked = {"philippines", "smoothcomp", "novice", "championship", "participants"}
        if any(b in lower for b in blocked):
            return False
        return bool(re.match(r"^[A-Za-z'`.-]+(?:\s+[A-Za-z'`.-]+){1,4}$", n))

    @staticmethod
    def _extract_people_from_text(text: str) -> list[str]:
        names = re.findall(r"\b([A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){1,4})\b", text)
        out: list[str] = []
        seen: set[str] = set()
        for name in names:
            candidate = re.sub(r"\s+", " ", name).strip()
            if not SmoothcompClient._is_plausible_person_name(candidate):
                continue
            key = candidate.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(candidate)
        return out

    @staticmethod
    def _extract_divisionish_text(text: str) -> str:
        lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines() if ln.strip()]
        for line in lines:
            low = line.lower()
            if "/" in line and any(k in low for k in ["male", "female", "adult", "master", "gi", "no-gi", "kg"]):
                return line[:120]
        m = re.search(
            r"((?:Male|Female)\s+[^\\n]{3,120}?(?:Gi|No-?Gi)[^\\n]{0,120})",
            text,
            re.IGNORECASE,
        )
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip()[:120]
        return ""

    @staticmethod
    def _name_from_url(url: str) -> str:
        tail = url.rstrip("/").rsplit("/", 1)[-1]
        if tail.isdigit():
            return f"Event {tail}"
        return re.sub(r"[-_]+", " ", tail).strip().title() or "Unnamed event"

    @staticmethod
    def _canonical_event_url(url: str) -> str:
        url = url.split("?", 1)[0].rstrip("/")
        match = re.search(r"(https?://[^/]+/(?:[a-z]{2}/)?event/\d+)", url, re.IGNORECASE)
        if match:
            return match.group(1)
        return url

    @staticmethod
    def _is_generic_event_name(name: str) -> bool:
        normalized = re.sub(r"\s+", " ", name).strip().lower()
        return not normalized or normalized in _GENERIC_EVENT_NAMES

    @staticmethod
    def _extract_name_from_text_blob(text: str) -> str:
        cleaned = re.sub(r"\s+", " ", text).strip()
        if not cleaned:
            return ""
        patterns = [
            rf"([A-Za-z0-9&'().,\- ]{{8,140}}?)\s+[A-Za-z .-]+,\s*Philippines\b",
            rf"([A-Za-z0-9&'().,\- ]{{8,140}}?)\s+(?:{_MONTHS_PATTERN})\s+\d{{1,2}}\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, cleaned, flags=re.IGNORECASE)
            if match:
                candidate = match.group(1).strip(" -|")
                if not SmoothcompClient._is_generic_event_name(candidate):
                    return candidate
        return ""

    def _extract_event_name_from_page(self, soup: BeautifulSoup, html: str, url: str) -> str:
        for selector in ("h1", "h2", "meta[property='og:title']", "title"):
            if selector.startswith("meta"):
                tag = soup.select_one(selector)
                if tag and tag.get("content"):
                    name = str(tag.get("content")).strip()
                    if not self._is_generic_event_name(name):
                        return name
                continue
            tag = soup.select_one(selector)
            if tag:
                if selector == "title":
                    name = tag.get_text(" ", strip=True)
                else:
                    name = " ".join(tag.stripped_strings).strip()
                if name:
                    name = re.sub(r"\s*\|\s*Smoothcomp.*$", "", name, flags=re.IGNORECASE)
                if name and not self._is_generic_event_name(name):
                    return name

        json_name = self._extract_name_from_context(html, url)
        if json_name and not self._is_generic_event_name(json_name):
            return json_name
        return self._name_from_url(url)

    def _extract_event_name_from_link(self, *, link: Tag, row_text: str, url: str) -> str:
        candidate_pool: list[str] = []

        link_text = " ".join(link.stripped_strings).strip()
        if link_text:
            candidate_pool.append(link_text)

        for attr in ("title", "aria-label", "data-title"):
            value = str(link.get(attr, "")).strip()
            if value:
                candidate_pool.append(value)

        container = link.find_parent(["article", "li", "tr", "section", "div"])
        scopes: list[Tag] = []
        if isinstance(container, Tag):
            scopes.append(container)
        if isinstance(link.parent, Tag):
            scopes.append(link.parent)

        for scope in scopes:
            for heading in scope.find_all(["h1", "h2", "h3", "h4", "h5", "strong", "b"]):
                text = " ".join(heading.stripped_strings).strip()
                if text:
                    candidate_pool.append(text)
            for img in scope.find_all("img", alt=True):
                alt = str(img.get("alt", "")).strip()
                if alt:
                    candidate_pool.append(alt)

        blob_name = self._extract_name_from_text_blob(row_text)
        if blob_name:
            candidate_pool.append(blob_name)

        for candidate in candidate_pool:
            normalized = re.sub(r"\s+", " ", candidate).strip()
            if len(normalized) < 6:
                continue
            if self._is_generic_event_name(normalized):
                continue
            return normalized
        return self._name_from_url(url)

    @staticmethod
    def _first_match(cells: list[str], markers: list[str]) -> str | None:
        for i, cell in enumerate(cells):
            lc = cell.lower()
            if any(marker in lc for marker in markers):
                if ":" in cell:
                    return cell.split(":", 1)[1].strip() or None
                if i + 1 < len(cells):
                    return cells[i + 1].strip() or None
        return None

    @staticmethod
    def _guess_affiliate(cells: list[str]) -> str:
        for cell in cells:
            if any(k in cell.lower() for k in ["academy", "affiliate", "team", "club"]):
                return cell
        return cells[1] if len(cells) > 1 else ""

    @staticmethod
    def _guess_name(cells: list[str]) -> str:
        candidates = [c for c in cells if re.match(r"^[A-Za-z\-\.,'\s]{5,}$", c)]
        if not candidates:
            return ""
        # Skip likely header-ish words.
        for c in candidates:
            lc = c.lower()
            if any(x in lc for x in ["academy", "division", "bracket", "mat", "ring", "time"]):
                continue
            return c.strip()
        return candidates[0].strip()

    @staticmethod
    def _guess_division(cells: list[str]) -> str:
        for cell in cells:
            lc = cell.lower()
            if any(x in lc for x in ["adult", "master", "juvenile", "kg", "lb", "white", "blue", "purple", "brown", "black"]):
                return cell
        return ""

    @staticmethod
    def _guess_time(cells: list[str]) -> str:
        time_pattern = re.compile(r"\b\d{1,2}:\d{2}\s*(?:am|pm)?\b", re.IGNORECASE)
        for cell in cells:
            m = time_pattern.search(cell)
            if m:
                return m.group(0)
        return ""

    @staticmethod
    def _guess_mat(cells: list[str]) -> str:
        mat_pattern = re.compile(r"\b(?:mat|ring|area)\s*#?\s*[A-Za-z0-9]+", re.IGNORECASE)
        for cell in cells:
            m = mat_pattern.search(cell)
            if m:
                return m.group(0)
        return ""

    @staticmethod
    def _extract_opponent(cells: list[str], competitor_name: str) -> str:
        vs_pattern = re.compile(r"(.+?)\s+vs\.?\s+(.+)", re.IGNORECASE)
        for cell in cells:
            m = vs_pattern.search(cell)
            if m:
                left = m.group(1).strip()
                right = m.group(2).strip()
                if competitor_name.lower() in left.lower():
                    return right
                if competitor_name.lower() in right.lower():
                    return left
        return "TBD"
