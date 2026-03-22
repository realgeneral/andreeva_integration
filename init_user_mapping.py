"""
Первичный скрипт инициализации соответствия пользователей МойСклад ↔ amoCRM.

Формат файла user_mapping_seed.json:
[
  {
    "moysklad_owner_id": "https://api.moysklad.ru/api/remap/1.2/entity/employee/...",
    "amocrm_user_id": 123456
  }
]

Запуск внутри контейнера (пример):
  docker compose run --rm web python init_user_mapping.py
"""

import json
from pathlib import Path
from typing import Any, Dict, List

from db import init_db, get_connection


SEED_FILE = Path(__file__).parent / "user_mapping_seed.json"


def load_seed() -> List[Dict[str, Any]]:
    if not SEED_FILE.exists():
        raise SystemExit(f"Файл с маппингом не найден: {SEED_FILE}")

    with SEED_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise SystemExit("user_mapping_seed.json должен содержать список объектов")
    return data


def init_user_mapping() -> None:
    init_db()
    seed = load_seed()

    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            for row in seed:
                ms_owner = str(row["moysklad_owner_id"])
                amo_user = int(row["amocrm_user_id"])
                cur.execute(
                    """
                    INSERT INTO user_mapping (moysklad_owner_id, amocrm_user_id)
                    VALUES (%s, %s)
                    ON CONFLICT (moysklad_owner_id)
                    DO UPDATE SET amocrm_user_id = EXCLUDED.amocrm_user_id;
                    """,
                    (ms_owner, amo_user),
                )
    finally:
        conn.close()


if __name__ == "__main__":
    init_user_mapping()
    print("user_mapping инициализирован из user_mapping_seed.json")

