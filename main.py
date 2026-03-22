import json
import logging
import re
from typing import Any, Dict, Optional
from urllib.parse import parse_qs

from fastapi import FastAPI, HTTPException, Request

from amo_add_lead_sync import process_amo_add_lead_owner_sync
from config import AMO_ADD_LEAD_WEBHOOK_ENABLED
from db import init_db
from logging_setup import setup_logging
from ms_to_amo_sync import sync_counterparty_by_href
from telegram_logger import notify_error, notify_startup


app = FastAPI(title="Andreeva Integration Service")
setup_logging("web")
logger = logging.getLogger("andreeva_integration")


@app.on_event("startup")
async def on_startup() -> None:
    # создаём таблицы, если их ещё нет
    init_db()
    logger.info(
        "Service startup: DB initialized, webhook app is starting | "
        "AMO_ADD_LEAD_WEBHOOK_ENABLED=%s",
        AMO_ADD_LEAD_WEBHOOK_ENABLED,
    )
    await notify_startup()


def _log_scenario_start(scenario: str, entity_id: str, source_ip: str) -> None:
    logger.info("[SCENARIO=%s] START | id=%s | source_ip=%s", scenario, entity_id, source_ip)


def _raw_preview(raw: bytes, limit: int = 600) -> str:
    """Безопасное короткое превью тела запроса для диагностики формата."""
    if not raw:
        return ""
    text = raw.decode("utf-8", errors="ignore").replace("\n", "\\n").replace("\r", "\\r")
    if len(text) > limit:
        return f"{text[:limit]}...(truncated)"
    return text


@app.post("/webhooks/moysklad/counterparty")
async def webhook_moysklad_counterparty(request: Request) -> Dict[str, str]:
    """
    Сценарий: МойСклад → amoCRM, синхронизация ответственного по контрагенту.
    Ожидается вебхук МойСклад на события CREATE/UPDATE сущности counterparty.
    """
    try:
        client_ip = request.client.host if request.client else "unknown"
        data = await request.json()
        event = data["events"][0]
        counterparty_href = event["meta"]["href"]
        _log_scenario_start("ms_to_amo_responsible_sync", counterparty_href, client_ip)
        return await sync_counterparty_by_href(counterparty_href, source="webhook")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unhandled exception in webhook_moysklad_counterparty")
        await notify_error(
            "Ошибка обработки вебхука МойСклад",
            details=str(exc),
            context="/webhooks/moysklad/counterparty",
        )
        raise HTTPException(status_code=500, detail="Internal webhook processing error") from exc


def _extract_lead_id_from_payload(data: Dict[str, Any]) -> Optional[int]:
    """Поддержка форматов вебхука amo v2 и v4."""
    try:
        if "leads" in data and "add" in data["leads"]:
            return int(data["leads"]["add"][0]["id"])
        if "leads" in data and isinstance(data["leads"], list) and data["leads"]:
            return int(data["leads"][0]["id"])
        if "_embedded" in data and "leads" in data["_embedded"]:
            return int(data["_embedded"]["leads"][0]["id"])
        if "lead_id" in data:
            return int(data["lead_id"])
        if "id" in data:
            return int(data["id"])
        # Плоские ключи после form/multipart, например: leads[add][0][id]
        if "leads[add][0][id]" in data:
            return int(data["leads[add][0][id]"])
        if "leads[status][0][id]" in data:
            return int(data["leads[status][0][id]"])
    except Exception:
        return None
    return None


async def _parse_amocrm_webhook_payload(request: Request) -> Dict[str, Any]:
    """
    amoCRM может присылать JSON или form-urlencoded.
    Нормализуем в словарь формата {"leads": {"add": [{"id": ...}]}}.
    """
    raw = await request.body()

    # 1) JSON
    if raw:
        try:
            return json.loads(raw.decode("utf-8", errors="ignore"))
        except Exception:
            pass

    # 2) form-urlencoded, пример ключа: leads[add][0][id]
    if raw:
        parsed = parse_qs(raw.decode("utf-8", errors="ignore"), keep_blank_values=True)
        lead_values = parsed.get("leads[add][0][id]") or parsed.get("leads[status][0][id]")
        if lead_values and lead_values[0]:
            return {"leads": {"add": [{"id": lead_values[0]}]}}
        # Вернем плоский словарь как fallback для _extract_lead_id_from_payload
        flat: Dict[str, Any] = {k: v[0] if v else "" for k, v in parsed.items()}
        if flat:
            return flat

    # 3) multipart/form-data или application/x-www-form-urlencoded через Starlette form parser
    try:
        form = await request.form()
        if form:
            form_dict = {k: v for k, v in form.items()}
            lead = form_dict.get("leads[add][0][id]") or form_dict.get("leads[status][0][id]")
            if lead:
                return {"leads": {"add": [{"id": str(lead)}]}}
            return form_dict
    except Exception:
        pass

    # 4) Совсем fallback: вытащить id регуляркой из сырого body
    if raw:
        text = raw.decode("utf-8", errors="ignore")
        match = re.search(r"leads\[(?:add|status)\]\[\d+\]\[id\]=(\d+)", text)
        if match:
            return {"leads": {"add": [{"id": match.group(1)}]}}

    return {}


@app.post("/webhooks/amocrm/add_lead")
async def webhook_amocrm_add_lead(request: Request) -> Dict[str, str]:
    """
    Сценарий: amoCRM → МойСklad.
    Вебхук на создание сделки (add_lead): ищем контрагента в МС и
    меняем ответственного в сделке по owner контрагента.

    Тот же сценарий без вебхука: опрос amoCRM по updated_at — контейнер amo_leads_poller,
    см. amo_leads_poll_worker.py. Включение вебхука: AMO_ADD_LEAD_WEBHOOK_ENABLED=true в .env.
    """
    try:
        if not AMO_ADD_LEAD_WEBHOOK_ENABLED:
            raise HTTPException(
                status_code=410,
                detail="Webhook add_lead отключён (AMO_ADD_LEAD_WEBHOOK_ENABLED). "
                "Обработка сделок — через amo_leads_poller.",
            )
        client_ip = request.client.host if request.client else "unknown"
        raw = await request.body()
        content_type = request.headers.get("content-type", "-")
        logger.info(
            "[SCENARIO=amo_add_lead_owner_sync] WEBHOOK_INCOMING | content_type=%s body_len=%s body_preview=%s",
            content_type,
            len(raw or b""),
            _raw_preview(raw),
        )
        data = await _parse_amocrm_webhook_payload(request)
        logger.info(
            "[SCENARIO=amo_add_lead_owner_sync] WEBHOOK_PARSED | payload_keys=%s",
            list(data.keys()),
        )

        lead_id = _extract_lead_id_from_payload(data)

        if not lead_id:
            await notify_error(
                "Ошибка вебхука amoCRM add_lead",
                details="Не удалось извлечь lead_id",
                context=(
                    f"content_type={content_type}, payload_keys={list(data.keys())}, "
                    f"body_preview={_raw_preview(raw, limit=300)}"
                ),
            )
            raise HTTPException(status_code=400, detail="Cannot extract lead_id from webhook payload")
        _log_scenario_start("amo_add_lead_owner_sync", str(lead_id), client_ip)
        return await process_amo_add_lead_owner_sync(
            lead_id,
            source="webhook",
            source_ip=client_ip,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unhandled exception in webhook_amocrm_add_lead")
        await notify_error(
            "Ошибка обработки add_lead",
            details=str(exc),
            context="/webhooks/amocrm/add_lead",
        )
        raise HTTPException(status_code=500, detail="Internal webhook processing error") from exc

