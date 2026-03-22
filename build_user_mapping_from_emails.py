"""
Скрипт для построения user_mapping_seed.json на основе соответствия email'ов
пользователей в amoCRM и МойСклад.

Использует токены из .env (через config.py).

Шаги использования:
1. Убедись, что .env заполнен (AMO_ACCESS_TOKEN, MOYSKLAD_TOKEN, ...).
2. При необходимости откорректируй список MAPPING_EMAILS ниже.
3. Запусти:
      python build_user_mapping_from_emails.py
4. В корне проекта появится user_mapping_seed.json.
5. Затем выполни:
      python init_user_mapping.py
   чтобы залить данные в таблицу user_mapping.
"""

import json
from dataclasses import dataclass
from typing import Dict, List, Optional

import httpx

from config import AMO_ACCESS_TOKEN, AMO_BASE_URL, MOYSKLAD_BASE_URL, MOYSKLAD_TOKEN


@dataclass
class EmailPair:
    amocrm_email: str
    moysklad_email: str


# TODO: при необходимости можно вынести в отдельный JSON/CSV;
# сейчас забито прямо из таблицы, которую ты прислал.
MAPPING_EMAILS: List[EmailPair] = [
    EmailPair("vika.andreeva@gmail.com", "vika.andreeva@gmail.com"),
    EmailPair("alinnaboris@yandex.ru", "ytrser@icloud.com"),
    EmailPair("dermaeliteoks@gmail.com", "89500240525l@gmail.com"),
    EmailPair("vi.estetschool@gmail.com", "larisaproba71@gmail.com"),
    EmailPair("ostroverkhova.aly@yandex.ru", "ostroverkhova.aly@yandex.ru"),
    EmailPair("ms_morozova.marina@mail.ru", "ms_morozova.marina@mail.ru"),
    EmailPair("kur-dina@mail.ru", "kur-dina@mail.ru"),
    EmailPair("Reyter.anastasia@gmail.com", "reyter.anastasia@gmail.com"),
]


def fetch_amocrm_users() -> Dict[str, int]:
    """
    Получить всех пользователей amoCRM и вернуть маппинг email → id.
    """
    if not AMO_ACCESS_TOKEN:
        raise SystemExit("AMO_ACCESS_TOKEN не задан в .env")

    headers = {
        "Authorization": f"Bearer {AMO_ACCESS_TOKEN}",
        "Accept": "application/json",
    }
    email_to_id: Dict[str, int] = {}

    url = f"{AMO_BASE_URL}/api/v4/users"
    with httpx.Client(timeout=15) as client:
        while url:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            for user in data.get("_embedded", {}).get("users", []):
                email = (user.get("email") or "").strip().lower()
                if email:
                    email_to_id[email] = int(user["id"])
            next_link = data.get("_links", {}).get("next", {}).get("href")
            url = next_link

    return email_to_id


def fetch_moysklad_employees() -> Dict[str, str]:
    """
    Получить сотрудников МойСклад и вернуть маппинг email → owner_id (meta.href).
    """
    if not MOYSKLAD_TOKEN:
        raise SystemExit("MOYSKLAD_TOKEN не задан в .env")

    headers = {
        "Authorization": f"Bearer {MOYSKLAD_TOKEN}",
        "Accept": "application/json;charset=utf-8",
        "Accept-Encoding": "gzip",
    }
    email_to_owner: Dict[str, str] = {}

    # Простой вариант без пагинации; при необходимости можно расширить.
    url = f"{MOYSKLAD_BASE_URL.rstrip('/')}/entity/employee"
    with httpx.Client(timeout=15) as client:
        params = {"limit": 1000}
        resp = client.get(url, headers=headers, params=params)
        if resp.status_code >= 400:
            print("Ошибка при запросе сотрудников МойСклад:")
            print(resp.status_code, resp.text)
            resp.raise_for_status()
        data = resp.json()
        for emp in data.get("rows", []):
            email = _extract_moysklad_employee_email(emp)
            if not email:
                continue
            meta = emp.get("meta", {})
            href = meta.get("href")
            if href:
                email_to_owner[email] = href

    return email_to_owner


def _extract_moysklad_employee_email(emp: dict) -> Optional[str]:
    """
    Попытаться вытащить email сотрудника МойСклад.
    Структура может отличаться, поэтому пробуем несколько мест.
    """
    # Прямое поле
    email = emp.get("email")
    if isinstance(email, str) and email:
        return email.strip().lower()

    # Через attributes (например, с именем "E-mail" / "Email")
    for attr in emp.get("attributes", []):
        name = (attr.get("name") or "").lower()
        if name in ("email", "e-mail", "e_mail", "почта"):
            val = attr.get("value")
            if isinstance(val, str) and val:
                return val.strip().lower()

    return None


def main() -> None:
    print("Загружаем пользователей amoCRM...")
    amo_users = fetch_amocrm_users()
    print(f"Найдено пользователей amoCRM: {len(amo_users)}")

    print("Загружаем сотрудников МойСклад...")
    ms_employees = fetch_moysklad_employees()
    print(f"Найдено сотрудников МойСклад: {len(ms_employees)}")

    result = []
    for pair in MAPPING_EMAILS:
        amo_email = pair.amocrm_email.strip().lower()
        ms_email = pair.moysklad_email.strip().lower()

        amo_id = amo_users.get(amo_email)
        ms_owner = ms_employees.get(ms_email)

        if not amo_id:
            print(f"[WARN] Не найден пользователь amoCRM c email={pair.amocrm_email}")
            continue
        if not ms_owner:
            print(f"[WARN] Не найден сотрудник МойСклад c email={pair.moysklad_email}")
            continue

        result.append(
            {
                "moysklad_owner_id": ms_owner,
                "amocrm_user_id": amo_id,
            }
        )

    if not result:
        raise SystemExit("Не удалось сформировать ни одной пары соответствия.")

    out_path = "user_mapping_seed.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Сохранено {len(result)} соответствий в {out_path}")
    print("Теперь можно запустить: python init_user_mapping.py")


if __name__ == "__main__":
    main()

