"""Telegram bot for sending alerts."""

import asyncio
import logging
from typing import Optional

from config import Config
from i18n.translations import PLAYER_ALERT_KEYS, t

logger = logging.getLogger(__name__)

# Rate limit: max 30 messages per second to a chat
SEND_DELAY = 0.5  # seconds between messages


class TelegramNotifier:
    def __init__(self):
        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.languages = Config.LANGUAGES or ["en"]
        self.bot = None
        token = Config.TELEGRAM_BOT_TOKEN
        if token:
            from telegram import Bot
            self.bot = Bot(token=token)
        else:
            logger.warning("TELEGRAM_BOT_TOKEN empty — alerts will be logged only")

    async def send(self, text: str, parse_mode: Optional[str] = None) -> bool:
        """Send a message to the configured chat (or log if Telegram is unset)."""
        if not self.bot or not self.chat_id:
            logger.info("TELEGRAM (log-only): %s", text.replace("\n", " | ")[:400])
            return True
        from telegram.error import RetryAfter, TelegramError

        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            return True
        except RetryAfter as e:
            logger.warning(f"Rate limited, retrying in {e.retry_after}s")
            await asyncio.sleep(e.retry_after)
            return await self.send(text, parse_mode)
        except TelegramError as e:
            logger.error(f"Telegram send error: {e}")
            return False

    async def send_multilingual(self, key: str, **kwargs) -> None:
        """Send a message in all configured languages."""
        for lang in self.languages:
            msg = t(key, lang=lang, **kwargs)
            await self.send(msg)
            await asyncio.sleep(SEND_DELAY)

    async def send_startup(self, watchlist_count: int = 0) -> None:
        """Send bot startup notification."""
        for lang in self.languages:
            msg = t(
                "bot_started",
                lang=lang,
                interval=Config.CHECK_INTERVAL_MINUTES,
                threshold=Config.ALERT_THRESHOLD_BPS,
                watchlist_count=watchlist_count,
            )
            await self.send(msg)
            await asyncio.sleep(SEND_DELAY)

    async def send_gap_alert(self, alert) -> None:
        """Send a gap alert in all languages."""
        for lang in self.languages:
            title = t("gap_alert_title", lang=lang, market_name=alert.market_name)
            body = t(
                "gap_alert_body",
                lang=lang,
                market_name=alert.market_name,
                category=alert.category,
                poly_price=alert.poly_price,
                kalshi_price=alert.kalshi_price,
                gap_bps=alert.gap_bps,
                direction=alert.direction,
                poly_url=alert.poly_url,
                kalshi_url=alert.kalshi_url,
            )
            await self.send(f"{title}\n\n{body}")
            await asyncio.sleep(SEND_DELAY)

    async def send_big_move_alert(self, alert) -> None:
        """Send a big move alert in all languages."""
        for lang in self.languages:
            title = t("big_move_title", lang=lang, market_name=alert.market_name)
            body = t(
                "big_move_body",
                lang=lang,
                market_name=alert.market_name,
                category=alert.category,
                source=alert.source,
                old_price=alert.old_price,
                new_price=alert.new_price,
                delta_bps=alert.delta_bps,
                timeframe=alert.timeframe,
                url=alert.url,
            )
            await self.send(f"{title}\n\n{body}")
            await asyncio.sleep(SEND_DELAY)

    async def send_rss_alert(self, item) -> None:
        """Send an RSS news alert in all languages."""
        for lang in self.languages:
            title = t("rss_alert_title", lang=lang, feed_name=item.feed_name)
            body = t(
                "rss_alert_body",
                lang=lang,
                title=item.title,
                summary=item.summary[:300],
                link=item.link,
            )
            await self.send(f"{title}\n\n{body}")
            await asyncio.sleep(SEND_DELAY)

    async def send_player_alert(self, signal, fill) -> None:
        """Send a player-intel / paper-fill alert in all languages."""
        title_key, body_key = PLAYER_ALERT_KEYS.get(
            signal.kind, ("player_trade_title", "player_trade_body")
        )
        kwargs = {
            "label": signal.label,
            "title": signal.title,
            "outcome": signal.outcome,
            "side": signal.side,
            "size": signal.size,
            "price": signal.price,
            "notional": signal.notional,
            "pnl": signal.pnl,
            "rank": signal.rank or "?",
            "vol": signal.vol,
            "profile_url": signal.profile_url,
            "market_url": signal.market_url or signal.profile_url,
            "paper_side": fill.side or "NOTE",
            "paper_size": fill.size,
            "paper_price": fill.price,
            "paper_notional": fill.notional,
        }
        for lang in self.languages:
            title = t(title_key, lang=lang, **kwargs)
            body = t(body_key, lang=lang, **kwargs)
            await self.send(f"{title}\n\n{body}")
            await asyncio.sleep(SEND_DELAY)

    async def send_heartbeat(
        self,
        market_count: int,
        feed_count: int,
        watchlist_count: int = 0,
        paper_fills: int = 0,
        paper_open_notional: float = 0.0,
    ) -> None:
        """Send a heartbeat / alive message."""
        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        msg = t(
            "heartbeat",
            lang=self.languages[0],
            timestamp=ts,
            market_count=market_count,
            feed_count=feed_count,
            watchlist_count=watchlist_count,
            paper_fills=paper_fills,
            paper_open_notional=paper_open_notional,
        )
        await self.send(msg)

    async def send_no_alerts(self) -> None:
        """Send a 'no alerts' message (only in first language)."""
        msg = t("no_alerts", lang=self.languages[0])
        await self.send(msg)

    async def send_error(self, error_msg: str) -> None:
        """Send an error notification."""
        msg = t("error", lang=self.languages[0], error_msg=str(error_msg)[:500])
        await self.send(msg)
