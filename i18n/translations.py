"""Multi-language support for alerts (EN / HE / FR)."""

TRANSLATIONS = {
    "en": {
        "bot_started": (
            "🚀 Market Intelligence Bot Started!\n\n"
            "🔍 Status: Active (paper player tracking)\n"
            "⏰ Frequency: Every {interval} min\n"
            "📊 Sources: Polymarket, Kalshi, RSS, player Data API\n"
            "🎯 Alert threshold: {threshold}+ bps\n"
            "👤 Watchlist wallets: {watchlist_count}\n"
            "📝 Mode: PAPER ONLY — no live orders\n"
            "🌐 Languages: EN, HE, FR"
        ),
        "gap_alert_title": "🔔 GAP ALERT — {market_name}",
        "gap_alert_body": (
            "📊 Market: {market_name}\n"
            "🏷️ Category: {category}\n\n"
            "Polymarket: {poly_price}%\n"
            "Kalshi: {kalshi_price}%\n"
            "📐 Gap: {gap_bps} bps\n"
            "📈 Direction: {direction}\n\n"
            "🔗 Poly: {poly_url}\n"
            "🔗 Kalshi: {kalshi_url}"
        ),
        "rss_alert_title": "📰 {feed_name} — New Update",
        "rss_alert_body": "📌 {title}\n\n{summary}\n\n🔗 {link}",
        "big_move_title": "⚡ BIG MOVE — {market_name}",
        "big_move_body": (
            "📊 {market_name}\n"
            "🏷️ Category: {category}\n"
            "Source: {source}\n\n"
            "Before: {old_price}% → Now: {new_price}%\n"
            "📐 Move: {delta_bps} bps\n"
            "⏱️ Timeframe: {timeframe}\n\n"
            "🔗 {url}"
        ),
        "player_open_title": "👤 PLAYER OPEN — {label}",
        "player_open_body": (
            "🟢 Watched wallet opened a notable position\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 Outcome: {outcome}\n"
            "💵 Size: {size:.2f} @ {price:.3f} (${notional:.0f})\n"
            "📝 Paper fill: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ Paper only — no live order placed\n\n"
            "🔗 Profile: {profile_url}\n"
            "🔗 Market: {market_url}"
        ),
        "player_close_title": "👤 PLAYER CLOSE — {label}",
        "player_close_body": (
            "🔴 Watched wallet closed a notable position\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 Outcome: {outcome}\n"
            "💵 Closed notional: ${notional:.0f} @ {price:.3f}\n"
            "📝 Paper fill: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ Paper only — no live order placed\n\n"
            "🔗 Profile: {profile_url}\n"
            "🔗 Market: {market_url}"
        ),
        "player_increase_title": "👤 PLAYER ADD — {label}",
        "player_increase_body": (
            "🟡 Watched wallet added size\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 Outcome: {outcome}\n"
            "💵 Now: {size:.2f} @ {price:.3f} (${notional:.0f})\n"
            "📝 Paper fill: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ Paper only — no live order placed\n\n"
            "🔗 Profile: {profile_url}\n"
            "🔗 Market: {market_url}"
        ),
        "player_trade_title": "👤 PLAYER TRADE — {label}",
        "player_trade_body": (
            "💱 Notable fill from a watched wallet\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 {side} {outcome}\n"
            "💵 {size:.2f} @ {price:.3f} (${notional:.0f})\n"
            "📝 Paper fill: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ Paper only — no live order placed\n\n"
            "🔗 Profile: {profile_url}\n"
            "🔗 Market: {market_url}"
        ),
        "leaderboard_unusual_title": "🏆 LEADERBOARD — {label}",
        "leaderboard_unusual_body": (
            "📣 Unusual size on the Polymarket leaderboard\n"
            "👤 {label}  (rank #{rank})\n"
            "📈 Volume: ${vol:.0f}\n"
            "💰 PnL: ${pnl:.0f}\n"
            "📝 Paper note recorded (no market fill)\n"
            "⚠️ Paper only — no live order placed\n\n"
            "🔗 Profile: {profile_url}"
        ),
        "heartbeat": (
            "💓 Bot alive — {timestamp}\n"
            "Markets tracked: {market_count}\n"
            "Feeds monitored: {feed_count}\n"
            "Wallets watched: {watchlist_count}\n"
            "Paper fills: {paper_fills} (open ${paper_open_notional:.0f})"
        ),
        "error": "⚠️ Error: {error_msg}",
        "no_alerts": "✅ Scan complete — no significant gaps, moves, or player signals.",
    },
    "he": {
        "bot_started": (
            "🚀 בוט מודיעין שווקים הופעל!\n\n"
            "🔍 מצב: פעיל (מעקב שחקנים בנייר)\n"
            "⏰ תדירות: כל {interval} דקות\n"
            "📊 מקורות: Polymarket, Kalshi, RSS, Data API\n"
            "🎯 סף התראה: {threshold}+ נ\"ב\n"
            "👤 ארנקים במעקב: {watchlist_count}\n"
            "📝 מצב: נייר בלבד — אין פקודות חיות\n"
            "🌐 שפות: EN, HE, FR"
        ),
        "gap_alert_title": "🔔 התראת פער — {market_name}",
        "gap_alert_body": (
            "📊 שוק: {market_name}\n"
            "🏷️ קטגוריה: {category}\n\n"
            "Polymarket: {poly_price}%\n"
            "Kalshi: {kalshi_price}%\n"
            "📐 פער: {gap_bps} נ\"ב\n"
            "📈 כיוון: {direction}\n\n"
            "🔗 Poly: {poly_url}\n"
            "🔗 Kalshi: {kalshi_url}"
        ),
        "rss_alert_title": "📰 {feed_name} — עדכון חדש",
        "rss_alert_body": "📌 {title}\n\n{summary}\n\n🔗 {link}",
        "big_move_title": "⚡ תנועה גדולה — {market_name}",
        "big_move_body": (
            "📊 {market_name}\n"
            "🏷️ קטגוריה: {category}\n"
            "מקור: {source}\n\n"
            "לפני: {old_price}% → עכשיו: {new_price}%\n"
            "📐 תנועה: {delta_bps} נ\"ב\n"
            "⏱️ טווח זמן: {timeframe}\n\n"
            "🔗 {url}"
        ),
        "player_open_title": "👤 שחקן פתח — {label}",
        "player_open_body": (
            "🟢 ארנק במעקב פתח פוזיציה משמעותית\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 תוצאה: {outcome}\n"
            "💵 גודל: {size:.2f} @ {price:.3f} (${notional:.0f})\n"
            "📝 מילוי נייר: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ נייר בלבד — לא בוצעה פקודה חיה\n\n"
            "🔗 פרופיל: {profile_url}\n"
            "🔗 שוק: {market_url}"
        ),
        "player_close_title": "👤 שחקן סגר — {label}",
        "player_close_body": (
            "🔴 ארנק במעקב סגר פוזיציה משמעותית\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 תוצאה: {outcome}\n"
            "💵 סגירה: ${notional:.0f} @ {price:.3f}\n"
            "📝 מילוי נייר: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ נייר בלבד — לא בוצעה פקודה חיה\n\n"
            "🔗 פרופיל: {profile_url}\n"
            "🔗 שוק: {market_url}"
        ),
        "player_increase_title": "👤 שחקן הוסיף — {label}",
        "player_increase_body": (
            "🟡 ארנק במעקב הגדיל פוזיציה\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 תוצאה: {outcome}\n"
            "💵 עכשיו: {size:.2f} @ {price:.3f} (${notional:.0f})\n"
            "📝 מילוי נייר: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ נייר בלבד — לא בוצעה פקודה חיה\n\n"
            "🔗 פרופיל: {profile_url}\n"
            "🔗 שוק: {market_url}"
        ),
        "player_trade_title": "👤 עסקת שחקן — {label}",
        "player_trade_body": (
            "💱 מילוי משמעותי מארנק במעקב\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 {side} {outcome}\n"
            "💵 {size:.2f} @ {price:.3f} (${notional:.0f})\n"
            "📝 מילוי נייר: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ נייר בלבד — לא בוצעה פקודה חיה\n\n"
            "🔗 פרופיל: {profile_url}\n"
            "🔗 שוק: {market_url}"
        ),
        "leaderboard_unusual_title": "🏆 לוח מובילים — {label}",
        "leaderboard_unusual_body": (
            "📣 גודל חריג בלוח המובילים של Polymarket\n"
            "👤 {label}  (דירוג #{rank})\n"
            "📈 מחזור: ${vol:.0f}\n"
            "💰 רווח/הפסד: ${pnl:.0f}\n"
            "📝 נרשמה הערת נייר (ללא מילוי שוק)\n"
            "⚠️ נייר בלבד — לא בוצעה פקודה חיה\n\n"
            "🔗 פרופיל: {profile_url}"
        ),
        "heartbeat": (
            "💓 הבוט פעיל — {timestamp}\n"
            "שווקים במעקב: {market_count}\n"
            "פידים במעקב: {feed_count}\n"
            "ארנקים במעקב: {watchlist_count}\n"
            "מילויי נייר: {paper_fills} (פתוח ${paper_open_notional:.0f})"
        ),
        "error": "⚠️ שגיאה: {error_msg}",
        "no_alerts": "✅ סריקה הושלמה — לא זוהו פערים, תנועות או אותות שחקנים.",
    },
    "fr": {
        "bot_started": (
            "🚀 Bot Intelligence Marchés Activé!\n\n"
            "🔍 Statut: Actif (suivi joueurs papier)\n"
            "⏰ Fréquence: Toutes les {interval} min\n"
            "📊 Sources: Polymarket, Kalshi, RSS, Data API\n"
            "🎯 Seuil d'alerte: {threshold}+ pdb\n"
            "👤 Wallets suivis: {watchlist_count}\n"
            "📝 Mode: PAPIER UNIQUEMENT — aucun ordre réel\n"
            "🌐 Langues: EN, HE, FR"
        ),
        "gap_alert_title": "🔔 ALERTE ÉCART — {market_name}",
        "gap_alert_body": (
            "📊 Marché: {market_name}\n"
            "🏷️ Catégorie: {category}\n\n"
            "Polymarket: {poly_price}%\n"
            "Kalshi: {kalshi_price}%\n"
            "📐 Écart: {gap_bps} pdb\n"
            "📈 Direction: {direction}\n\n"
            "🔗 Poly: {poly_url}\n"
            "🔗 Kalshi: {kalshi_url}"
        ),
        "rss_alert_title": "📰 {feed_name} — Nouvelle mise à jour",
        "rss_alert_body": "📌 {title}\n\n{summary}\n\n🔗 {link}",
        "big_move_title": "⚡ MOUVEMENT IMPORTANT — {market_name}",
        "big_move_body": (
            "📊 {market_name}\n"
            "🏷️ Catégorie: {category}\n"
            "Source: {source}\n\n"
            "Avant: {old_price}% → Maintenant: {new_price}%\n"
            "📐 Mouvement: {delta_bps} pdb\n"
            "⏱️ Période: {timeframe}\n\n"
            "🔗 {url}"
        ),
        "player_open_title": "👤 JOUEUR OUVERTURE — {label}",
        "player_open_body": (
            "🟢 Un wallet suivi a ouvert une position notable\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 Issue: {outcome}\n"
            "💵 Taille: {size:.2f} @ {price:.3f} (${notional:.0f})\n"
            "📝 Fill papier: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ Papier uniquement — aucun ordre réel\n\n"
            "🔗 Profil: {profile_url}\n"
            "🔗 Marché: {market_url}"
        ),
        "player_close_title": "👤 JOUEUR CLÔTURE — {label}",
        "player_close_body": (
            "🔴 Un wallet suivi a clôturé une position notable\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 Issue: {outcome}\n"
            "💵 Notionnel clôturé: ${notional:.0f} @ {price:.3f}\n"
            "📝 Fill papier: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ Papier uniquement — aucun ordre réel\n\n"
            "🔗 Profil: {profile_url}\n"
            "🔗 Marché: {market_url}"
        ),
        "player_increase_title": "👤 JOUEUR AJOUT — {label}",
        "player_increase_body": (
            "🟡 Un wallet suivi a augmenté sa taille\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 Issue: {outcome}\n"
            "💵 Maintenant: {size:.2f} @ {price:.3f} (${notional:.0f})\n"
            "📝 Fill papier: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ Papier uniquement — aucun ordre réel\n\n"
            "🔗 Profil: {profile_url}\n"
            "🔗 Marché: {market_url}"
        ),
        "player_trade_title": "👤 TRADE JOUEUR — {label}",
        "player_trade_body": (
            "💱 Fill notable d'un wallet suivi\n"
            "👤 {label}\n"
            "📊 {title}\n"
            "🎯 {side} {outcome}\n"
            "💵 {size:.2f} @ {price:.3f} (${notional:.0f})\n"
            "📝 Fill papier: {paper_side} {paper_size:.2f} @ {paper_price:.3f} (${paper_notional:.2f})\n"
            "⚠️ Papier uniquement — aucun ordre réel\n\n"
            "🔗 Profil: {profile_url}\n"
            "🔗 Marché: {market_url}"
        ),
        "leaderboard_unusual_title": "🏆 CLASSEMENT — {label}",
        "leaderboard_unusual_body": (
            "📣 Taille inhabituelle sur le classement Polymarket\n"
            "👤 {label}  (rang #{rank})\n"
            "📈 Volume: ${vol:.0f}\n"
            "💰 PnL: ${pnl:.0f}\n"
            "📝 Note papier enregistrée (pas de fill marché)\n"
            "⚠️ Papier uniquement — aucun ordre réel\n\n"
            "🔗 Profil: {profile_url}"
        ),
        "heartbeat": (
            "💓 Bot en vie — {timestamp}\n"
            "Marchés suivis: {market_count}\n"
            "Flux surveillés: {feed_count}\n"
            "Wallets suivis: {watchlist_count}\n"
            "Fills papier: {paper_fills} (ouvert ${paper_open_notional:.0f})"
        ),
        "error": "⚠️ Erreur: {error_msg}",
        "no_alerts": "✅ Scan terminé — aucun écart, mouvement ou signal joueur significatif.",
    },
}


PLAYER_ALERT_KEYS = {
    "open": ("player_open_title", "player_open_body"),
    "close": ("player_close_title", "player_close_body"),
    "increase": ("player_increase_title", "player_increase_body"),
    "trade": ("player_trade_title", "player_trade_body"),
    "leaderboard": ("leaderboard_unusual_title", "leaderboard_unusual_body"),
}


def t(key: str, lang: str = "en", **kwargs) -> str:
    """Get translated string with formatting."""
    template = TRANSLATIONS.get(lang, TRANSLATIONS["en"]).get(key)
    if template is None:
        template = TRANSLATIONS["en"].get(key, key)
    try:
        return template.format(**kwargs)
    except (KeyError, ValueError):
        return template
