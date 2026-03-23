import logging
from typing import Any, Dict, Optional

import httpx

from config import AMO_BASE_URL, AMO_ACCESS_TOKEN, AMO_INN_FIELD_ID

logger = logging.getLogger("andreeva_integration")


def _auth_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AMO_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _amo_raise_for_status(resp: httpx.Response) -> None:
    """
    Аналог raise_for_status: при HTTP 400 логирует и добавляет тело ответа amo в текст исключения.
    """
    if resp.is_success:
        return
    if resp.status_code == 400:
        body = (resp.text or "").strip()
        logger.error(
            "amoCRM HTTP 400 | %s %s | response.text=%s",
            resp.request.method,
            resp.url,
            body if body else "(пусто)",
        )
        raise httpx.HTTPStatusError(
            f"Client error '400 Bad Request' for url '{resp.url}'\n"
            f"Amo response body:\n{body}",
            request=resp.request,
            response=resp,
        )
    resp.raise_for_status()


async def find_company_by_inn(inn: str) -> Optional[Dict[str, Any]]:
    """
    Рабочий поиск компании по ИНН для текущего аккаунта amoCRM:
    используем query-поиск (фильтр custom_fields_values в этом аккаунте возвращает 400).
    """
    inn_digits = "".join(ch for ch in str(inn) if ch.isdigit())
    if not inn_digits:
        return None

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{AMO_BASE_URL}/api/v4/companies",
            headers=_auth_headers(),
            params={"query": inn_digits},
        )
        if resp.status_code == 204:
            return None
        resp.raise_for_status()
        items = resp.json().get("_embedded", {}).get("companies", [])
        if not items:
            return None

        # Если задано поле ИНН, стараемся выбрать точное совпадение по значению поля.
        if AMO_INN_FIELD_ID:
            try:
                inn_field_id = int(AMO_INN_FIELD_ID)
            except (TypeError, ValueError):
                inn_field_id = None

            if inn_field_id is not None:
                for company in items:
                    for cf in (company.get("custom_fields_values") or []):
                        if cf.get("field_id") != inn_field_id:
                            continue
                        for val in (cf.get("values") or []):
                            candidate = "".join(ch for ch in str(val.get("value", "")) if ch.isdigit())
                            if candidate and candidate == inn_digits:
                                return company

        # fallback: берем первый результат query
        logger.info("Amo INN query fallback used (no exact CF match) for inn=%s", inn_digits)
        return items[0]


async def find_company_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    """
    Поиск компании по номеру телефона (через общий поиск query).
    Предполагается, что телефон хранится в стандартном поле PHONE
    или в одном из индексируемых полей компании.
    """
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{AMO_BASE_URL}/api/v4/companies",
            headers=_auth_headers(),
            params={"query": phone},
        )
        if resp.status_code == 204:
            return None
        resp.raise_for_status()
        items = resp.json().get("_embedded", {}).get("companies", [])
        return items[0] if items else None


async def find_contact_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    """Поиск контакта по номеру телефона (через query)."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{AMO_BASE_URL}/api/v4/contacts",
            headers=_auth_headers(),
            params={"query": phone},
        )
        if resp.status_code == 204:
            return None
        resp.raise_for_status()
        items = resp.json().get("_embedded", {}).get("contacts", [])
        return items[0] if items else None


async def fetch_lead_ids_since(
    from_unix: int,
    *,
    date_field: str = "created_at",
    limit: int = 250,
) -> list[tuple[int, int]]:
    """
    Сделки с date_field >= from_unix (Unix сек., filter inclusive в amo).
    date_field: created_at — только новые сделки; updated_at — любые изменения.
    Возвращает (lead_id, ts) где ts — то же поле для watermark (max + 1).
    """
    if date_field not in ("created_at", "updated_at"):
        raise ValueError("date_field must be created_at or updated_at")
    out: list[tuple[int, int]] = []
    page = 1
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            params: list[tuple[str, str]] = [
                (f"filter[{date_field}][from]", str(from_unix)),
                (f"order[{date_field}]", "asc"),
                ("limit", str(limit)),
                ("page", str(page)),
            ]
            resp = await client.get(
                f"{AMO_BASE_URL}/api/v4/leads",
                headers=_auth_headers(),
                params=params,
            )
            if page == 1:
                logger.info(
                    "amo GET /api/v4/leads date_field=%s page=1 status=%s (204 = пусто)",
                    date_field,
                    resp.status_code,
                )
            if resp.status_code == 204:
                break
            resp.raise_for_status()
            data = resp.json()
            leads = data.get("_embedded", {}).get("leads", [])
            if not leads:
                break
            for L in leads:
                lid = int(L["id"])
                ts = int(L.get(date_field) or 0)
                out.append((lid, ts))
            if len(leads) < limit:
                break
            page += 1
    return out


async def fetch_lead_ids_updated_since(
    updated_from_unix: int,
    *,
    limit: int = 250,
) -> list[tuple[int, int]]:
    """Обёртка для совместимости: фильтр по updated_at."""
    return await fetch_lead_ids_since(updated_from_unix, date_field="updated_at", limit=limit)


async def get_lead_with_links(lead_id: int) -> Dict[str, Any]:
    """Получить сделку с привязанными контактами и компаниями."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{AMO_BASE_URL}/api/v4/leads/{lead_id}",
            headers=_auth_headers(),
            params={"with": "contacts,companies"},
        )
        resp.raise_for_status()
        return resp.json()


async def list_users(*, limit: int = 250) -> list[Dict[str, Any]]:
    """Список пользователей аккаунта amoCRM (id, name, email, …)."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{AMO_BASE_URL}/api/v4/users",
            headers=_auth_headers(),
            params={"limit": str(limit)},
        )
        if resp.status_code == 204:
            return []
        resp.raise_for_status()
        return resp.json().get("_embedded", {}).get("users", [])


async def get_leads_pipelines() -> list[Dict[str, Any]]:
    """Воронки и статусы сделок (type: 0 обычный, 1 успех, 2 провал)."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{AMO_BASE_URL}/api/v4/leads/pipelines",
            headers=_auth_headers(),
        )
        resp.raise_for_status()
        return resp.json().get("_embedded", {}).get("pipelines", [])


def terminal_status_ids_from_pipelines(pipelines: list[Dict[str, Any]]) -> set[int]:
    """
    ID закрывающих статусов (успех / провал).

    В amo у «Неразобранного» часто type=1 — это НЕ закрытие сделки.
    Системные этапы с id 142 / 143 — типичные «успех» и «провал» во многих аккаунтах.
    Дополнительно учитываем type=2 там, где он означает провал.
    """
    closed: set[int] = set()
    for pipe in pipelines:
        for st in pipe.get("_embedded", {}).get("statuses", []) or []:
            try:
                sid = int(st["id"])
            except (TypeError, ValueError, KeyError):
                continue
            if sid in (142, 143):
                closed.add(sid)
                continue
            if st.get("type") == 2:
                closed.add(sid)
    return closed


async def fetch_leads_by_responsible_user(
    responsible_user_id: int,
    *,
    limit: int = 250,
) -> list[Dict[str, Any]]:
    """
    Все сделки, где ответственный = responsible_user_id (постранично).
    В ответе списка обычно есть custom_fields_values (в т.ч. ссылка МС).
    """
    page = 1
    out: list[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            params: list[tuple[str, str]] = [
                ("filter[responsible_user_id][]", str(responsible_user_id)),
                ("limit", str(limit)),
                ("page", str(page)),
            ]
            resp = await client.get(
                f"{AMO_BASE_URL}/api/v4/leads",
                headers=_auth_headers(),
                params=params,
            )
            if resp.status_code == 204:
                break
            resp.raise_for_status()
            leads = resp.json().get("_embedded", {}).get("leads", [])
            if not leads:
                break
            out.extend(leads)
            if len(leads) < limit:
                break
            page += 1
    return out


async def get_company(company_id: int) -> Dict[str, Any]:
    """Получить компанию amoCRM по ID."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{AMO_BASE_URL}/api/v4/companies/{company_id}",
            headers=_auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()


async def get_contact(contact_id: int) -> Dict[str, Any]:
    """Получить контакт amoCRM по ID."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{AMO_BASE_URL}/api/v4/contacts/{contact_id}",
            headers=_auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()


async def update_responsible(
    entity: str,
    entity_id: int,
    user_id: int,
) -> None:
    """Обновить responsible_user_id для сущности amoCRM."""
    payload = {"responsible_user_id": user_id}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.patch(
            f"{AMO_BASE_URL}/api/v4/{entity}/{entity_id}",
            headers=_auth_headers(),
            json=payload,
        )
        _amo_raise_for_status(resp)


async def link_contact_to_company(contact_id: int, company_id: int) -> None:
    """Явно привязать контакт к компании в amoCRM."""
    payload = [
        {
            "to_entity_id": company_id,
            "to_entity_type": "companies",
        }
    ]
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{AMO_BASE_URL}/api/v4/contacts/{contact_id}/link",
            headers=_auth_headers(),
            json=payload,
        )
        resp.raise_for_status()


async def create_contact(
    name: str,
    responsible_user_id: int,
    phone: Optional[str] = None,
    company_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Создать контакт в amoCRM, опционально привязав к компании."""
    contact: Dict[str, Any] = {
        "name": name,
        "responsible_user_id": responsible_user_id,
    }
    if phone:
        contact["custom_fields_values"] = [
            {
                "field_code": "PHONE",
                "values": [{"value": phone}],
            }
        ]

    if company_id:
        contact["_embedded"] = {"companies": [{"id": company_id}]}

    payload = [contact]
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{AMO_BASE_URL}/api/v4/contacts",
            headers=_auth_headers(),
            json=payload,
        )
        resp.raise_for_status()
        items = resp.json().get("_embedded", {}).get("contacts", [])
        if not items:
            raise RuntimeError("amoCRM не вернул созданный контакт")
        return items[0]


async def create_company(
    name: str,
    responsible_user_id: int,
    inn: Optional[str] = None,
) -> Dict[str, Any]:
    """Создать компанию в amoCRM без создания сделки."""
    company: Dict[str, Any] = {
        "name": name,
        "responsible_user_id": responsible_user_id,
    }
    if inn and AMO_INN_FIELD_ID:
        company["custom_fields_values"] = [
            {
                "field_id": int(AMO_INN_FIELD_ID),
                "values": [{"value": inn}],
            }
        ]

    payload = [company]
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{AMO_BASE_URL}/api/v4/companies",
            headers=_auth_headers(),
            json=payload,
        )
        resp.raise_for_status()
        items = resp.json().get("_embedded", {}).get("companies", [])
        if not items:
            raise RuntimeError("amoCRM не вернул созданную компанию")
        return items[0]


async def create_lead_complex_with_company_contact(
    lead_name: str,
    responsible_user_id: int,
    company_name: Optional[str] = None,
    inn: Optional[str] = None,
    contact_name: Optional[str] = None,
    phone: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Создание сделки + компании + контакта через /api/v4/leads/complex.
    Возвращает полный ответ amoCRM.
    """
    lead: Dict[str, Any] = {
        "name": lead_name,
        "responsible_user_id": responsible_user_id,
        "_embedded": {},
    }

    companies = []
    contacts = []

    if company_name:
        company: Dict[str, Any] = {
            "name": company_name,
            "responsible_user_id": responsible_user_id,
        }
        if inn and AMO_INN_FIELD_ID:
            company["custom_fields_values"] = [
                {
                    "field_id": int(AMO_INN_FIELD_ID),
                    "values": [{"value": inn}],
                }
            ]
        companies.append(company)

    if contact_name or phone:
        contact: Dict[str, Any] = {
            "name": contact_name or (company_name or "Контакт из МойСклад"),
            "responsible_user_id": responsible_user_id,
        }
        cf_values = []
        if phone:
            cf_values.append(
                {
                    "field_code": "PHONE",
                    "values": [{"value": phone}],
                }
            )
        if cf_values:
            contact["custom_fields_values"] = cf_values
        contacts.append(contact)

    if companies:
        lead["_embedded"]["companies"] = companies
    if contacts:
        lead["_embedded"]["contacts"] = contacts

    payload = [lead]

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{AMO_BASE_URL}/api/v4/leads/complex",
            headers=_auth_headers(),
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()

