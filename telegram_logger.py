from datetime import datetime
from typing import Optional

import httpx

from config import (
    APP_NAME,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_HTTP_PROXY,
    TELEGRAM_SEND_ERRORS,
    TELEGRAM_SEND_SKIPS,
    TELEGRAM_SEND_SUCCESS,
)


def _dt_now_str() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M:%S")


def _build_message(
    icon: str,
    title: str,
    details: Optional[str] = None,
    context: Optional[str] = None,
) -> str:
    parts = [
        APP_NAME,
        "",
        f"{icon}{_dt_now_str()}",
        "",
        title,
    ]
    if details:
        parts.extend(["", "Детали:", details])
    if context:
        parts.extend(["", "Контекст:", context])
    return "\n".join(parts)


async def _send_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        # Только TELEGRAM_HTTP_PROXY, без HTTP_PROXY/HTTPS_PROXY из окружения (amo/МС и т.д. не трогаем)
        client_kwargs: dict = {"timeout": 15, "trust_env": False}
        if TELEGRAM_HTTP_PROXY:
            client_kwargs["proxy"] = TELEGRAM_HTTP_PROXY
        async with httpx.AsyncClient(**client_kwargs) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        # Не валим основной сервис из-за недоступности/ошибки Telegram.
        print(f"[telegram_logger] send failed: {type(exc).__name__}: {exc!s}")


async def notify_startup() -> None:
    text = _build_message(
        "🚀",
        "Сервис web запущен. Вебхуки — если настроены URL; сделки amo — поллер (отдельный контейнер).",
    )
    await _send_message(text)


async def notify_success(title: str, details: Optional[str] = None, context: Optional[str] = None) -> None:
    if not TELEGRAM_SEND_SUCCESS:
        return
    text = _build_message("✅", title, details=details, context=context)
    await _send_message(text)


async def notify_error(title: str, details: Optional[str] = None, context: Optional[str] = None) -> None:
    if not TELEGRAM_SEND_ERRORS:
        return
    text = _build_message("🚨", title, details=details, context=context)
    await _send_message(text)


async def notify_skip(
    reason_code: str,
    title: str,
    details: Optional[str] = None,
    context: Optional[str] = None,
) -> None:
    """
    Уведомления по сценарию add_lead с явным кодом причины (как в массовых отчётах).
    reason_code: skip_no_phone_FIELD_CODE_PHONE | skip_ms_owner_not_found | skip_no_user_mapping_for_owner
    """
    if not TELEGRAM_SEND_SKIPS:
        return
    full_title = f"[{reason_code}] {title}"
    text = _build_message("⚠️", full_title, details=details, context=context)
    await _send_message(text)

