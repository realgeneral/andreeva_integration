from typing import Any, Dict, Optional, Tuple

import httpx
import json as pyjson

from config import MOYSKLAD_TOKEN


def _default_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {MOYSKLAD_TOKEN}",
        "Accept": "application/json;charset=utf-8",
        "Accept-Encoding": "gzip",
    }


async def get_counterparty_by_href(href: str) -> Dict[str, Any]:
    """Получить контрагента по meta.href из МойСклад."""
    headers = _default_headers()
    async with httpx.AsyncClient(timeout=15) as client:
        last_exc: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                resp = await client.get(href, headers=headers)
                resp.raise_for_status()
                return resp.json()
            except (pyjson.JSONDecodeError, ValueError) as exc:
                # Иногда API может вернуть пустое/некорректное тело при 200.
                last_exc = exc
                if attempt == 3:
                    content_type = resp.headers.get("content-type", "")
                    body_preview = (resp.text or "")[:300]
                    raise ValueError(
                        f"Invalid JSON from MoySklad for href={href}; "
                        f"status={resp.status_code}; content_type={content_type}; "
                        f"body_preview={body_preview}"
                    ) from exc
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt == 3:
                    raise
        if last_exc:
            raise last_exc
        raise RuntimeError("Unexpected error in get_counterparty_by_href")


async def list_updated_counterparty_hrefs(
    base_url: str,
    updated_from_iso: str,
    limit: int = 100,
) -> Tuple[list[str], Optional[str]]:
    """
    Вернуть href контрагентов, изменённых после updated_from_iso.
    """
    headers = _default_headers()
    endpoint = f"{base_url.rstrip('/')}/entity/counterparty"
    hrefs: list[str] = []
    max_updated: Optional[str] = None
    offset = 0

    updated_filter_value = _moysklad_filter_datetime(updated_from_iso)
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            params = {
                "filter": f"updated>{updated_filter_value}",
                "order": "updated,asc",
                "limit": limit,
                "offset": offset,
            }
            resp = await client.get(endpoint, headers=headers, params=params)
            resp.raise_for_status()
            rows = resp.json().get("rows", [])
            if not rows:
                break
            for row in rows:
                meta = row.get("meta", {})
                href = meta.get("href")
                if href:
                    hrefs.append(href)
                updated = row.get("updated")
                if isinstance(updated, str):
                    # Лексикографическое сравнение для одинакового формата datetime строк.
                    if max_updated is None or updated > max_updated:
                        max_updated = updated
            if len(rows) < limit:
                break
            offset += limit

    return hrefs, max_updated


def _moysklad_filter_datetime(iso_dt: str) -> str:
    """
    Привести ISO datetime к формату, который корректно ест фильтр МойСклад:
    YYYY-MM-DD HH:MM:SS
    """
    value = iso_dt.strip()
    if "T" in value:
        value = value.replace("T", " ")
    if "+" in value:
        value = value.split("+", 1)[0]
    if value.endswith("Z"):
        value = value[:-1]
    if "." in value:
        value = value.split(".", 1)[0]
    return value


async def find_counterparty_by_inn_or_phone(
    base_url: str,
    inn: Optional[str] = None,
    phone: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Поиск контрагента по ИНН или телефону.

    Важно: фильтры нужно будет подогнать под реальную модель в МС
    (как именно хранится телефон: в поле phone или в attributes).
    """
    if not inn and not phone:
        return None

    headers = _default_headers()
    endpoint = f"{base_url.rstrip('/')}/entity/counterparty"

    async with httpx.AsyncClient(timeout=15) as client:
        # Сначала пробуем по ИНН
        if inn:
            params = {"filter": f"inn={inn}"}
            resp = await client.get(endpoint, headers=headers, params=params)
            resp.raise_for_status()
            rows = resp.json().get("rows", [])
            if rows:
                return rows[0]

        # Потом по телефону (пробуем несколько нормализованных форматов)
        if phone:
            for phone_variant in _phone_variants(phone):
                params = {"filter": f"phone~={phone_variant}"}
                resp = await client.get(endpoint, headers=headers, params=params)
                resp.raise_for_status()
                rows = resp.json().get("rows", [])
                if rows:
                    return rows[0]

    return None


def _phone_variants(phone: str) -> list[str]:
    """
    Генерирует варианты номера для поиска в МойСклад:
    исходный, только цифры, +7..., 7..., 8..., 10 цифр.
    """
    raw = phone.strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    variants: list[str] = []

    def add(v: str) -> None:
        if v and v not in variants:
            variants.append(v)

    add(raw)
    add(digits)

    if len(digits) == 11:
        add(f"+{digits}")
        if digits.startswith("8"):
            d7 = "7" + digits[1:]
            add(d7)
            add("+" + d7)
            add(d7[1:])  # 10 цифр
        elif digits.startswith("7"):
            d8 = "8" + digits[1:]
            add(d8)
            add("+" + digits)
            add(digits[1:])  # 10 цифр

    return variants

