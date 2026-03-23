import os
from typing import Optional

from dotenv import load_dotenv

# Загружаем переменные из .env при локальном запуске
load_dotenv()


AMO_BASE_URL = os.getenv("AMO_BASE_URL", "https://dermaelite.amocrm.ru")
AMO_ACCESS_TOKEN = os.getenv("AMO_ACCESS_TOKEN")

MOYSKLAD_BASE_URL = os.getenv("MOYSKLAD_BASE_URL", "https://api.moysklad.ru/api/remap/1.2")
MOYSKLAD_TOKEN = os.getenv("MOYSKLAD_TOKEN")

# пример: postgresql://user:password@db:5432/biznesavtomatizator
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://biznes:biznes@db:5432/biznesavtomatizator",
)

# ID кастомного поля ИНН в amoCRM (число в виде строки)
AMO_INN_FIELD_ID = os.getenv("AMO_INN_FIELD_ID")

# ID поля сделки «Ссылка на заказ в МойСклад» — без заполненного значения сделку не обрабатываем
# По умолчанию 3257473; задать пустую строку в .env — отключить проверку
_raw_ms_order_field = os.getenv("AMO_MS_ORDER_LINK_FIELD_ID", "3257473")
AMO_MS_ORDER_LINK_FIELD_ID = (_raw_ms_order_field or "").strip() or None

APP_NAME = os.getenv("APP_NAME", "andreeva-integration")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_SEND_SUCCESS = os.getenv("TELEGRAM_SEND_SUCCESS", "false").lower() in ("1", "true", "yes", "on")
TELEGRAM_SEND_ERRORS = os.getenv("TELEGRAM_SEND_ERRORS", "true").lower() in ("1", "true", "yes", "on")
# Уведомления о «пропусках» add_lead: нет PHONE, нет контрагента/owner в МС, нет user_mapping
TELEGRAM_SEND_SKIPS = os.getenv("TELEGRAM_SEND_SKIPS", "true").lower() in ("1", "true", "yes", "on")
# Прокси только для вызовов api.telegram.org в рантайме (не используется при docker build / pip).
# Примеры: http://user:pass@host:3128 | socks5://user:pass@host:2080 (нужен httpx[socks] в requirements.txt)
TELEGRAM_HTTP_PROXY = (os.getenv("TELEGRAM_HTTP_PROXY") or "").strip() or None

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "120"))
POLL_INITIAL_LOOKBACK_MINUTES = int(os.getenv("POLL_INITIAL_LOOKBACK_MINUTES", "10"))

# Опрос сделок amoCRM — см. amo_leads_poll_worker.py (по умолчанию 5 мин)
AMO_LEADS_POLL_INTERVAL_SECONDS = int(os.getenv("AMO_LEADS_POLL_INTERVAL_SECONDS", "300"))
AMO_LEADS_POLL_LOOKBACK_MINUTES = int(os.getenv("AMO_LEADS_POLL_LOOKBACK_MINUTES", "10"))
# Поллер сделок: created_at = только новые сделки (по умолчанию); updated_at = любое изменение
_raw_poll_field = (os.getenv("AMO_LEADS_POLL_DATE_FIELD") or "created_at").strip().lower()
AMO_LEADS_POLL_DATE_FIELD = _raw_poll_field if _raw_poll_field in ("created_at", "updated_at") else "created_at"

# Вебхук POST /webhooks/amocrm/add_lead (по умолчанию выключён, если работает amo_leads_poller)
# Пользователь amo «интеграция / бизнесавтоматизатор» — для ручного бэкфилла см. scripts/backfill_integration_leads.py
_raw_amo_int_user = (os.getenv("AMO_INTEGRATION_RESPONSIBLE_USER_ID") or "").strip()
AMO_INTEGRATION_RESPONSIBLE_USER_ID: Optional[int] = (
    int(_raw_amo_int_user) if _raw_amo_int_user.isdigit() else None
)

AMO_ADD_LEAD_WEBHOOK_ENABLED = os.getenv("AMO_ADD_LEAD_WEBHOOK_ENABLED", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

LOG_DIR = os.getenv("LOG_DIR", "/app/logs")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "2"))

