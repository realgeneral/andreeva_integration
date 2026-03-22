"""
Синхронизация ответственного по сделке amoCRM из МойСклад (owner контрагента).
Используется и вебхуком /webhooks/amocrm/add_lead, и polling-воркером amo_leads_poll_worker.
"""
import logging
from typing import Any, Dict, Optional

from amocrm_client import (
    get_company,
    get_contact,
    get_lead_with_links,
    update_responsible,
)
from config import AMO_INN_FIELD_ID, MOYSKLAD_BASE_URL
from db import get_amocrm_user_id_by_ms_owner
from moysklad_client import find_counterparty_by_inn_or_phone
from telegram_logger import notify_skip, notify_success

logger = logging.getLogger("andreeva_integration")

SCENARIO = "amo_add_lead_owner_sync"


def _log_step(step: str, details: str) -> None:
    logger.info("[SCENARIO=%s] %s | %s", SCENARIO, step, details)


def _extract_inn_from_company(company: Dict[str, Any]) -> Optional[str]:
    if not AMO_INN_FIELD_ID:
        return None
    try:
        inn_field_id = int(AMO_INN_FIELD_ID)
    except (TypeError, ValueError):
        return None
    for cf in company.get("custom_fields_values") or []:
        if cf.get("field_id") != inn_field_id:
            continue
        for val in cf.get("values") or []:
            value = val.get("value")
            if value:
                return str(value)
    return None


def _extract_phone_from_contact(contact: Dict[str, Any]) -> Optional[str]:
    for cf in contact.get("custom_fields_values") or []:
        if cf.get("field_code") == "PHONE":
            for val in cf.get("values") or []:
                value = val.get("value")
                if value:
                    return str(value)
    return None


def _extract_phone_from_company(company: Dict[str, Any]) -> Optional[str]:
    for cf in company.get("custom_fields_values") or []:
        if cf.get("field_code") == "PHONE":
            for val in cf.get("values") or []:
                value = val.get("value")
                if value:
                    return str(value)
    return None


async def _notify_and_log_skip(
    status: str,
    reason_code: str,
    title: str,
    details: str,
    context: str,
) -> Dict[str, str]:
    logger.warning(
        "Non-success status=%s reason_code=%s | details=%s | context=%s",
        status,
        reason_code,
        details,
        context,
    )
    await notify_skip(reason_code, title, details=details, context=context)
    return {"status": status}


async def process_amo_add_lead_owner_sync(
    lead_id: int,
    *,
    source: str = "webhook",
    source_ip: str = "unknown",
) -> Dict[str, str]:
    """
    Та же логика, что и вебхук add_lead: МС → ответственный в сделке/компании/контакте.
    Возвращает {"status": "ok"} или словарь со status=skip_*; бросает исключение при сбоях API.
    """
    logger.info(
        "[SCENARIO=%s] START | id=%s | source=%s | source_ip=%s",
        SCENARIO,
        lead_id,
        source,
        source_ip,
    )

    lead = await get_lead_with_links(lead_id)
    embedded = lead.get("_embedded", {})
    companies = embedded.get("companies", [])
    contacts = embedded.get("contacts", [])

    company_id = int(companies[0]["id"]) if companies else None
    contact_id = int(contacts[0]["id"]) if contacts else None
    _log_step("READ_LEAD_LINKS", f"company_id={company_id}, contact_id={contact_id}")

    inn: Optional[str] = None
    company_phone: Optional[str] = None
    contact_phone: Optional[str] = None
    search_phone: Optional[str] = None

    if company_id:
        company_obj = await get_company(company_id)
        inn = _extract_inn_from_company(company_obj)
        company_phone = _extract_phone_from_company(company_obj)
        _log_step(
            "READ_COMPANY",
            f"company_id={company_id}, inn={inn}, company_phone={company_phone}",
        )

    if contact_id:
        contact_obj = await get_contact(contact_id)
        contact_phone = _extract_phone_from_contact(contact_obj)
        _log_step("READ_CONTACT", f"contact_id={contact_id}, contact_phone={contact_phone}")

    counterparty = await find_counterparty_by_inn_or_phone(
        base_url=MOYSKLAD_BASE_URL,
        inn=inn,
        phone=company_phone,
    )
    search_phone = company_phone
    _log_step(
        "SEARCH_MS_BY_COMPANY",
        f"inn={inn}, phone={company_phone}, found={bool(counterparty)}",
    )

    if not counterparty and contact_phone and contact_phone != company_phone:
        counterparty = await find_counterparty_by_inn_or_phone(
            base_url=MOYSKLAD_BASE_URL,
            inn=None,
            phone=contact_phone,
        )
        search_phone = contact_phone
        _log_step(
            "SEARCH_MS_BY_CONTACT_FALLBACK",
            f"phone={contact_phone}, found={bool(counterparty)}",
        )

    if not counterparty:
        if contact_id and contact_phone is None:
            await notify_skip(
                "skip_no_phone_FIELD_CODE_PHONE",
                "Нет телефона в contact.field_code=PHONE",
                details=f"lead_id={lead_id}, contact_id={contact_id}",
                context=(
                    f"inn={inn}, company_phone={company_phone}, "
                    f"contact_phone={contact_phone}, used_phone={search_phone}"
                ),
            )
        return await _notify_and_log_skip(
            status="no_counterparty_in_moysklad",
            reason_code="skip_ms_owner_not_found",
            title="Не найден контрагент в МойСклад / нет owner для поиска",
            details=f"lead_id={lead_id}",
            context=(
                f"inn={inn}, company_phone={company_phone}, "
                f"contact_phone={contact_phone}, used_phone={search_phone}"
            ),
        )

    owner_meta = counterparty.get("owner", {}).get("meta", {})
    owner_id = owner_meta.get("href") or owner_meta.get("id")
    if not owner_id:
        return await _notify_and_log_skip(
            status="no_owner_in_moysklad",
            reason_code="skip_ms_owner_not_found",
            title="Контрагент в МойСклад без owner",
            details=f"lead_id={lead_id}",
            context=(
                f"inn={inn}, company_phone={company_phone}, "
                f"contact_phone={contact_phone}, used_phone={search_phone}"
            ),
        )

    amocrm_user_id = get_amocrm_user_id_by_ms_owner(str(owner_id))
    if not amocrm_user_id:
        return await _notify_and_log_skip(
            status="no_user_mapping",
            reason_code="skip_no_user_mapping_for_owner",
            title="Нет строки user_mapping для owner из МойСклад",
            details=f"owner_id={owner_id}",
            context=f"lead_id={lead_id}",
        )
    logger.info(
        "[SCENARIO=%s] MAP_OWNER | lead_id=%s owner_id=%s amo_user_id=%s",
        SCENARIO,
        lead_id,
        owner_id,
        amocrm_user_id,
    )

    await update_responsible("leads", lead_id, amocrm_user_id)
    _log_step(
        "UPDATE_LEAD_RESPONSIBLE",
        f"lead_id={lead_id}, responsible_user_id={amocrm_user_id}",
    )

    if company_id:
        await update_responsible("companies", company_id, amocrm_user_id)
        _log_step(
            "UPDATE_COMPANY_RESPONSIBLE",
            f"company_id={company_id}, responsible_user_id={amocrm_user_id}",
        )
    if contact_id:
        await update_responsible("contacts", contact_id, amocrm_user_id)
        _log_step(
            "UPDATE_CONTACT_RESPONSIBLE",
            f"contact_id={contact_id}, responsible_user_id={amocrm_user_id}",
        )

    await notify_success(
        "Успешная обработка add_lead (amoCRM -> МойСклад)",
        details=f"Сделка: {lead_id} | источник: {source}",
        context=(
            f"owner_id={owner_id}, amo_user_id={amocrm_user_id}, "
            f"inn={inn}, company_phone={company_phone}, "
            f"contact_phone={contact_phone}, used_phone={search_phone}, "
            f"company_id={company_id}, contact_id={contact_id}"
        ),
    )
    logger.info(
        "[SCENARIO=%s] SUCCESS | lead_id=%s owner_id=%s amo_user_id=%s inn=%s company_phone=%s contact_phone=%s used_phone=%s source=%s",
        SCENARIO,
        lead_id,
        owner_id,
        amocrm_user_id,
        inn,
        company_phone,
        contact_phone,
        search_phone,
        source,
    )

    return {"status": "ok"}
