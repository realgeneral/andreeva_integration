"""
Опрос amoCRM: новые сделки (по created_at) или изменённые (updated_at) — см. AMO_LEADS_POLL_DATE_FIELD.
Та же обработка, что вебхук add_lead.

Запуск: контейнер amo_leads_poller в docker-compose.
"""
import asyncio
import logging
from datetime import timedelta, timezone

from amo_add_lead_sync import process_amo_add_lead_owner_sync
from amocrm_client import fetch_lead_ids_since
from config import (
    AMO_LEADS_POLL_DATE_FIELD,
    AMO_LEADS_POLL_INTERVAL_SECONDS,
    AMO_LEADS_POLL_LOOKBACK_MINUTES,
)
from db import get_sync_state, init_db, set_sync_state
from logging_setup import setup_logging
from telegram_logger import notify_error

# Разные ключи для created_at / updated_at, чтобы watermark не смешивался
SYNC_KEY = f"amo_leads_owner_poll_{AMO_LEADS_POLL_DATE_FIELD}_from"
logger = logging.getLogger("andreeva_integration.amo_leads_poll")


def _initial_from_ts() -> int:
    from datetime import datetime

    dt = datetime.now(timezone.utc) - timedelta(minutes=AMO_LEADS_POLL_LOOKBACK_MINUTES)
    return int(dt.timestamp())


async def run_once() -> None:
    raw = get_sync_state(SYNC_KEY)
    if raw:
        from_ts = int(raw)
    else:
        from_ts = _initial_from_ts()

    logger.info(
        "[AMO_LEADS_POLL] START | date_field=%s filter[%s][from]=%s (unix) sync_key=%s",
        AMO_LEADS_POLL_DATE_FIELD,
        AMO_LEADS_POLL_DATE_FIELD,
        from_ts,
        SYNC_KEY,
    )

    try:
        items = await fetch_lead_ids_since(from_ts, date_field=AMO_LEADS_POLL_DATE_FIELD)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[AMO_LEADS_POLL] FETCH_ERROR")
        await notify_error(
            "Ошибка запроса списка сделок amoCRM (polling)",
            details=str(exc),
            context=f"sync_key={SYNC_KEY}, from_ts={from_ts}, date_field={AMO_LEADS_POLL_DATE_FIELD}",
        )
        return

    logger.info("[AMO_LEADS_POLL] FETCH | leads=%s", len(items))

    ok = 0
    skipped = 0
    failed = 0

    for lead_id, ts in items:
        try:
            result = await process_amo_add_lead_owner_sync(
                lead_id,
                source="polling",
                source_ip="amo_leads_poll",
            )
            if result.get("status") == "ok":
                ok += 1
            else:
                skipped += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logger.exception("[AMO_LEADS_POLL] ITEM_ERROR | lead_id=%s", lead_id)
            await notify_error(
                "Ошибка обработки сделки (amo polling)",
                details=str(exc),
                context=f"lead_id={lead_id}, {AMO_LEADS_POLL_DATE_FIELD}={ts}",
            )

    if items:
        max_ts = max(t for _, t in items)
        new_from = max_ts + 1
        set_sync_state(SYNC_KEY, str(new_from))
        logger.info(
            "[AMO_LEADS_POLL] END | ok=%s skipped=%s failed=%s next_from_ts=%s (%s)",
            ok,
            skipped,
            failed,
            new_from,
            AMO_LEADS_POLL_DATE_FIELD,
        )
    else:
        if not raw:
            set_sync_state(SYNC_KEY, str(from_ts))
            logger.info(
                "[AMO_LEADS_POLL] END | empty first run, watermark=%s (saved from_ts) | ok=%s skipped=%s failed=%s",
                from_ts,
                ok,
                skipped,
                failed,
            )
        else:
            logger.info(
                "[AMO_LEADS_POLL] END | no leads in window | ok=%s skipped=%s failed=%s",
                ok,
                skipped,
                failed,
            )

    if failed > 0:
        await notify_error(
            "Проход опроса сделок amoCRM с ошибками",
            details=f"обработано={len(items)}, ok={ok}, skipped={skipped}, failed={failed}",
            context=f"from_ts={from_ts}, date_field={AMO_LEADS_POLL_DATE_FIELD}",
        )


async def main() -> None:
    setup_logging("amo_leads_poller")
    init_db()
    logger.info(
        "[AMO_LEADS_POLL] Worker started | interval_seconds=%s lookback_minutes=%s date_field=%s",
        AMO_LEADS_POLL_INTERVAL_SECONDS,
        AMO_LEADS_POLL_LOOKBACK_MINUTES,
        AMO_LEADS_POLL_DATE_FIELD,
    )
    while True:
        try:
            await run_once()
        except Exception as exc:  # noqa: BLE001
            logger.exception("[AMO_LEADS_POLL] LOOP_ERROR")
            await notify_error(
                "Критическая ошибка amo leads polling",
                details=str(exc),
                context="amo_leads_poll_worker.py main loop",
            )
        await asyncio.sleep(AMO_LEADS_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
