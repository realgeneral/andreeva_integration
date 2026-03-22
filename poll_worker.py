import asyncio
import logging
from datetime import datetime, timedelta, timezone

from config import MOYSKLAD_BASE_URL, POLL_INITIAL_LOOKBACK_MINUTES, POLL_INTERVAL_SECONDS
from db import get_sync_state, init_db, set_sync_state
from logging_setup import setup_logging
from moysklad_client import list_updated_counterparty_hrefs
from ms_to_amo_sync import sync_counterparty_by_href
from telegram_logger import notify_error

SYNC_KEY = "ms_counterparty_last_sync_iso"
logger = logging.getLogger("andreeva_integration.poller")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _initial_sync_iso() -> str:
    dt = datetime.now(timezone.utc) - timedelta(minutes=POLL_INITIAL_LOOKBACK_MINUTES)
    return dt.isoformat()


def _sync_from_ms_updated(ms_updated: str) -> str:
    """
    Преобразует updated из МС (обычно 'YYYY-MM-DD HH:MM:SS[.ms]') в ISO UTC-подобный watermark
    и добавляет +1 секунду, чтобы не зацикливаться на одной и той же отметке.
    """
    raw = ms_updated.split(".")[0]
    dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc) + timedelta(seconds=1)
    return dt.isoformat()


async def run_once() -> None:
    last_sync_iso = get_sync_state(SYNC_KEY) or _initial_sync_iso()
    logger.info("[POLLING] START | last_sync_iso=%s", last_sync_iso)

    hrefs, max_updated = await list_updated_counterparty_hrefs(MOYSKLAD_BASE_URL, last_sync_iso)
    logger.info("[POLLING] FETCH | updated_counterparties=%s", len(hrefs))

    ok = 0
    failed = 0
    for href in hrefs:
        try:
            result = await sync_counterparty_by_href(href, source="polling")
            if result.get("status") == "ok":
                ok += 1
            else:
                failed += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logger.exception("[POLLING] ITEM_ERROR | href=%s", href)
            await notify_error(
                "Ошибка polling синхронизации МойСклад -> amoCRM",
                details=str(exc),
                context=f"counterparty_href={href}",
            )

    if max_updated:
        new_sync_iso = _sync_from_ms_updated(max_updated)
    else:
        # Если изменений нет, не двигаем watermark.
        # Иначе можно случайно "откатиться" по часовому поясу и снова получить те же записи.
        new_sync_iso = last_sync_iso
    set_sync_state(SYNC_KEY, new_sync_iso)
    logger.info("[POLLING] END | ok=%s failed=%s new_sync_iso=%s max_updated=%s", ok, failed, new_sync_iso, max_updated)
    if failed > 0:
        await notify_error(
            "Polling проход с ошибками",
            details=f"обработано={len(hrefs)}, ok={ok}, failed={failed}",
            context=f"last_sync={last_sync_iso} -> new_sync={new_sync_iso}, max_updated={max_updated}",
        )


async def main() -> None:
    setup_logging("poller")
    init_db()
    logger.info("[POLLING] Worker started | interval_seconds=%s", POLL_INTERVAL_SECONDS)

    while True:
        try:
            await run_once()
        except Exception as exc:  # noqa: BLE001
            logger.exception("[POLLING] LOOP_ERROR")
            await notify_error(
                "Критическая ошибка polling-воркера",
                details=str(exc),
                context="poll_worker.py main loop",
            )
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())

