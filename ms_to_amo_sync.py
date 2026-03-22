import logging
from typing import Any, Dict, Optional

from amocrm_client import (
    create_company,
    create_contact,
    find_company_by_inn,
    find_company_by_phone,
    find_contact_by_phone,
    link_contact_to_company,
    update_responsible,
)
from db import get_amocrm_user_id_by_ms_owner
from moysklad_client import get_counterparty_by_href
from telegram_logger import notify_error, notify_success

logger = logging.getLogger("andreeva_integration")


def _extract_phone_from_counterparty(counterparty: Dict[str, Any]) -> Optional[str]:
    phone = counterparty.get("phone")
    if isinstance(phone, str):
        return phone
    for attr in counterparty.get("attributes", []):
        if attr.get("name", "").lower() in ("телефон", "phone"):
            return str(attr.get("value"))
    return None


async def sync_counterparty_by_href(counterparty_href: str, source: str = "webhook") -> Dict[str, str]:
    """
    Синхронизация ответственного из МС в amo по href контрагента.
    source: webhook | polling
    """
    scenario = "ms_to_amo_responsible_sync"
    logger.info("[SCENARIO=%s] START | source=%s | href=%s", scenario, source, counterparty_href)

    cp = await get_counterparty_by_href(counterparty_href)
    owner_meta = cp.get("owner", {}).get("meta", {})
    owner_id = owner_meta.get("href") or owner_meta.get("id")
    if not owner_id:
        await notify_error(
            "Контрагент без owner в МойСклад",
            details=f"source={source}",
            context=f"counterparty_href={counterparty_href}",
        )
        logger.warning("[SCENARIO=%s] no_owner_in_moysklad | href=%s", scenario, counterparty_href)
        return {"status": "no_owner_in_moysklad"}

    inn = cp.get("inn")
    phone = _extract_phone_from_counterparty(cp)
    amocrm_user_id = get_amocrm_user_id_by_ms_owner(str(owner_id))
    if not amocrm_user_id:
        await notify_error(
            "Не найдено соответствие пользователя",
            details=f"owner_id={owner_id}",
            context=f"source={source}, href={counterparty_href}",
        )
        logger.warning("[SCENARIO=%s] no_user_mapping | owner_id=%s", scenario, owner_id)
        return {"status": "no_user_mapping"}

    company: Optional[Dict[str, Any]] = None
    contact: Optional[Dict[str, Any]] = None

    if phone:
        contact = await find_contact_by_phone(phone)
        if contact:
            companies = contact.get("_embedded", {}).get("companies", [])
            if companies:
                company = companies[0]
        if not company:
            company = await find_company_by_phone(phone)

    if not company and inn:
        company = await find_company_by_inn(inn)

    # По новому правилу создаем сущности только если есть телефон.
    # Сделку при автосоздании не создаем.
    if not company and not contact and phone:
        company = await create_company(
            name=cp.get("name") or "Контрагент из МойСклад",
            responsible_user_id=amocrm_user_id,
            inn=inn,
        )
        contact = await create_contact(
            name=cp.get("name") or "Контакт из МойСклад",
            responsible_user_id=amocrm_user_id,
            phone=phone,
            company_id=int(company["id"]),
        )

    if company and not contact and phone:
        contact = await create_contact(
            name=cp.get("name") or "Контакт из МойСклад",
            responsible_user_id=amocrm_user_id,
            phone=phone,
            company_id=int(company["id"]),
        )

    if company and contact:
        await link_contact_to_company(int(contact["id"]), int(company["id"]))

    if company:
        await update_responsible("companies", int(company["id"]), amocrm_user_id)
    if contact:
        await update_responsible("contacts", int(contact["id"]), amocrm_user_id)

    if source != "polling":
        await notify_success(
            "Успешная синхронизация ответственного (МС -> amoCRM)",
            details=f"source={source}, company_id={company['id'] if company else '-'}, contact_id={contact['id'] if contact else '-'}",
            context=f"owner_id={owner_id}, amo_user_id={amocrm_user_id}, inn={inn}, phone={phone}",
        )
    logger.info(
        "[SCENARIO=%s] SUCCESS | source=%s owner_id=%s amo_user_id=%s company_id=%s contact_id=%s",
        scenario,
        source,
        owner_id,
        amocrm_user_id,
        company["id"] if company else "-",
        contact["id"] if contact else "-",
    )
    return {"status": "ok"}

