from __future__ import annotations

import logging
import re
from datetime import datetime
from html import escape

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, NetworkError, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from .config import Settings
from .formatters import format_competitors, format_events
from .models import Event
from .smoothcomp import SmoothcompClient

EVENT_CACHE_KEY = "events_cache"
LOGGER = logging.getLogger(__name__)
EVENT_URL_RE = re.compile(r"https?://[^\s]*smoothcomp\.com/(?:[^\s]*/)?event/\d+(?:/[^\s?#]+)?", re.IGNORECASE)
EVENT_ID_RE = re.compile(r"\bevent\s*id\s*[:#-]?\s*(\d{4,9})\b", re.IGNORECASE)
YEAR_RE = re.compile(r"\b(20\d{2})\b")


class TournaBot:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.smoothcomp = SmoothcompClient(
            base_events_url=settings.smoothcomp_events_url,
            fallback_events_url=settings.smoothcomp_events_fallback_url,
            timeout_seconds=settings.smoothcomp_timeout_seconds,
        )
        self.app = Application.builder().token(settings.telegram_bot_token).build()
        self._register_handlers()

    def _register_handlers(self) -> None:
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("help", self.help_cmd))
        self.app.add_handler(CommandHandler("events", self.events))
        self.app.add_handler(CommandHandler("upcoming", self.events))
        self.app.add_handler(CommandHandler("schedule", self.events))
        self.app.add_handler(CommandHandler("debugevents", self.debug_events))
        self.app.add_handler(CallbackQueryHandler(self.on_event_selected, pattern=r"^event:"))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text_input))
        self.app.add_error_handler(self.on_error)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "Send me an event reference:\n"
            "1) Event URL (e.g. https://bjjfphilippines.smoothcomp.com/en/event/26935)\n"
            "2) Event ID (e.g. 26935 or `event id: 26935`)\n"
            "3) Event name + year (e.g. `Hyperfly Asian Open 2026`)\n\n"
            "If you send a name, I will show candidate events for confirmation.",
            parse_mode=ParseMode.MARKDOWN,
        )

    async def help_cmd(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "How to use:\n"
            "- Send event URL directly\n"
            "- Send event ID directly\n"
            "- Send event name + year and pick the correct match\n"
            "\n"
            "Extra commands:\n"
            "- /events 2026 (list discoverable events)\n"
            "- /debugevents 2026",
        )

    async def events(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        msg = update.message
        if msg is None:
            return

        year = datetime.now().year
        if context.args:
            try:
                year = int(context.args[0])
            except ValueError:
                await msg.reply_text("Invalid year. Example: /events 2026")
                return

        await msg.reply_text("Fetching events from Smoothcomp. Please wait...")

        events = await self.smoothcomp.fetch_events(
            year=year,
            country=self.settings.smoothcomp_default_country,
        )

        context.bot_data[EVENT_CACHE_KEY] = {event.id: event for event in events}

        keyboard = [
            [InlineKeyboardButton(text=e.name[:60], callback_data=f"event:{e.id}")]
            for e in events[:25]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None

        await msg.reply_text(
            format_events(events),
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
        )

    async def handle_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        msg = update.message
        if msg is None or not msg.text:
            return

        text = msg.text.strip()
        if not text:
            return

        url = self._extract_event_url(text)
        if url:
            await msg.reply_text("Got event URL. Loading competitors and schedules...")
            event = await self.smoothcomp.fetch_event_by_url(url, self.settings.smoothcomp_default_country)
            if event is None:
                await msg.reply_text("I could not load that event URL. Please check and send again.")
                return
            context.bot_data.setdefault(EVENT_CACHE_KEY, {})[event.id] = event
            await self._send_event_schedule(event=event, context=context, chat_id=msg.chat_id)
            return

        event_id = self._extract_event_id(text)
        if event_id:
            await msg.reply_text("Got event ID. Loading event details...")
            event = await self.smoothcomp.fetch_event_by_id(event_id, self.settings.smoothcomp_default_country)
            if event is None:
                await msg.reply_text("I could not load that event ID. Please verify the ID and try again.")
                return
            context.bot_data.setdefault(EVENT_CACHE_KEY, {})[event.id] = event
            await self._send_event_schedule(event=event, context=context, chat_id=msg.chat_id)
            return

        query, year = self._parse_name_query(text)
        await msg.reply_text("Searching matching events. Please wait...")
        matches = await self.smoothcomp.search_events_by_name(
            name_query=query,
            year=year,
            country=self.settings.smoothcomp_default_country,
        )
        if not matches:
            await msg.reply_text(
                "No matching events found. Send an event URL or event ID for exact lookup."
            )
            return

        context.bot_data[EVENT_CACHE_KEY] = {event.id: event for event in matches}
        keyboard = [
            [InlineKeyboardButton(text=e.name[:60], callback_data=f"event:{e.id}")]
            for e in matches[:10]
        ]
        await msg.reply_text(
            "I found these events. Please confirm the correct one:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    async def on_event_selected(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None or query.data is None:
            return

        await query.answer()
        event_id = query.data.split(":", 1)[1]
        event_map: dict[str, Event] = context.bot_data.get(EVENT_CACHE_KEY, {})
        event = event_map.get(event_id)

        if event is None:
            await query.edit_message_text(
                "I could not find that event in cache. Send URL/ID again or run /events."
            )
            return

        await query.edit_message_text(
            f"Loading schedules for {event.name}...",
        )

        await self._send_event_schedule(event=event, context=context, chat_id=query.message.chat_id)

    async def _send_event_schedule(
        self,
        *,
        event: Event,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
    ) -> None:
        competitors = await self.smoothcomp.fetch_competitors_for_event(
            event,
            affiliate_keywords=self.settings.team_affiliate_keywords,
        )

        text = format_competitors(
            event=event,
            competitor_rows=competitors,
            affiliate_keywords=self.settings.team_affiliate_keywords,
        )
        if not competitors:
            affiliate_hints = await self.smoothcomp.detect_affiliates_for_event(event, limit=12)
            people_hints = await self.smoothcomp.detect_people_for_event(event, limit=15)
            if affiliate_hints:
                hint_lines = "\n".join(f"- {escape(name)}" for name in affiliate_hints)
                text += (
                    "\n\n<b>Detected affiliate labels on this event</b>\n"
                    "Try adding one of these to <code>TEAM_AFFILIATE_KEYWORDS</code>:\n"
                    f"{hint_lines}"
                )
            if people_hints:
                people_lines = "\n".join(
                    f"- {escape(name)} ({escape(division)})" for name, division in people_hints
                )
                text += (
                    "\n\n<b>Detected competitors on this event</b>\n"
                    "These names were found, but none matched your team filter:\n"
                    f"{people_lines}"
                )

        await self._safe_send_message(context=context, chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)

    async def debug_events(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        msg = update.message
        if msg is None:
            return
        year = datetime.now().year
        if context.args:
            try:
                year = int(context.args[0])
            except ValueError:
                await msg.reply_text("Invalid year. Example: /debugevents 2026")
                return

        await msg.reply_text("Running Smoothcomp event discovery debug...")
        text = await self.smoothcomp.debug_event_discovery(
            year=year,
            country=self.settings.smoothcomp_default_country,
        )
        await self._safe_send_message(
            context=context,
            chat_id=msg.chat_id,
            text=f"<pre>{escape(text)}</pre>",
            parse_mode=ParseMode.HTML,
        )

    async def on_error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        LOGGER.exception("Unhandled Telegram update error: %s", context.error)
        if isinstance(context.error, BadRequest):
            LOGGER.error("Telegram BadRequest details: %s", context.error)

    async def _safe_send_message(
        self,
        *,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        text: str,
        parse_mode: str | None = None,
    ) -> None:
        chunks = self._split_message(text, limit=3500)
        for chunk in chunks:
            try:
                await context.bot.send_message(chat_id=chat_id, text=chunk, parse_mode=parse_mode)
            except BadRequest as exc:
                LOGGER.warning("HTML send failed, retrying plain text. Reason: %s", exc)
                plain = self._strip_html(chunk)
                for plain_chunk in self._split_message(plain, limit=3900):
                    await context.bot.send_message(chat_id=chat_id, text=plain_chunk)

    @staticmethod
    def _split_message(text: str, limit: int = 3500) -> list[str]:
        if len(text) <= limit:
            return [text]
        out: list[str] = []
        remaining = text
        while len(remaining) > limit:
            cut = remaining.rfind("\n", 0, limit)
            if cut < limit // 2:
                cut = limit
            out.append(remaining[:cut].strip())
            remaining = remaining[cut:].strip()
        if remaining:
            out.append(remaining)
        return out

    @staticmethod
    def _strip_html(text: str) -> str:
        text = text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
        text = re.sub(r"</?(?:b|i|u|strong|em|code|pre)>", "", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        return text

    @staticmethod
    def _extract_event_url(text: str) -> str | None:
        m = EVENT_URL_RE.search(text)
        return m.group(0) if m else None

    @staticmethod
    def _extract_event_id(text: str) -> str | None:
        m = EVENT_ID_RE.search(text)
        if m:
            return m.group(1)
        trimmed = text.strip()
        if trimmed.isdigit() and 4 <= len(trimmed) <= 9:
            return trimmed
        return None

    @staticmethod
    def _parse_name_query(text: str) -> tuple[str, int]:
        year = datetime.now().year
        m = YEAR_RE.search(text)
        query = text
        if m:
            year = int(m.group(1))
        return query.strip(), year

    def run(self) -> None:
        LOGGER.info("Starting TournaBot polling loop...")
        try:
            self.app.run_polling()
        except NetworkError as exc:
            LOGGER.error(
                "Could not reach Telegram API. Check internet/DNS/proxy/firewall. Details: %s",
                exc,
            )
            raise
        except TelegramError as exc:
            LOGGER.error("Telegram API error while starting bot: %s", exc)
            raise
