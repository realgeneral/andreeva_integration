#!/usr/bin/env python3
"""
Проставление ответственных в amo по owner из МойСклад (тот же сценарий, что поллер).

Обязателен флаг --execute (защита от случайного запуска).

Примеры:
  python3 scripts/apply_leads_execute.py --execute --user-id 12050794
  python3 scripts/apply_leads_execute.py --execute   # user-id из AMO_INTEGRATION_RESPONSIBLE_USER_ID в .env

Без спама в Telegram на время прогона:
  TELEGRAM_SEND_SUCCESS=false TELEGRAM_SEND_SKIPS=false python3 scripts/apply_leads_execute.py --execute --user-id 12050794

Полный dry-run (ничего не меняет в amo):
  python3 scripts/backfill_integration_leads.py report --user-id 12050794
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_backfill():
    path = ROOT / "scripts" / "backfill_integration_leads.py"
    spec = importlib.util.spec_from_file_location("_backfill_integration_leads", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Не удалось загрузить {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> None:
    p = argparse.ArgumentParser(
        description="Бэкфилл: МойСклад → ответственный в amo (нужен --execute).",
    )
    p.add_argument(
        "--execute",
        action="store_true",
        required=True,
        help="Подтверждение: выполнить PATCH в amoCRM и обновить ответственных",
    )
    p.add_argument(
        "--user-id",
        type=int,
        default=None,
        help="ID пользователя amo «интеграция» (иначе AMO_INTEGRATION_RESPONSIBLE_USER_ID из .env)",
    )
    p.add_argument(
        "--include-closed",
        action="store_true",
        help="Включить закрытые сделки (142/143); по умолчанию только активные",
    )
    p.add_argument(
        "--today-moscow",
        action="store_true",
        help="Только сделки с updated_at с полуночи сегодня (МСК)",
    )
    p.add_argument(
        "--updated-from-unix",
        type=int,
        default=None,
        help="Только сделки с updated_at >= unix timestamp",
    )
    p.add_argument(
        "--max-leads",
        type=int,
        default=None,
        help="Обработать не больше N сделок",
    )
    args = p.parse_args()

    ns = SimpleNamespace(
        user_id=args.user_id,
        include_closed=args.include_closed,
        today_moscow=args.today_moscow,
        updated_from_unix=args.updated_from_unix,
        max_leads=args.max_leads,
        execute=args.execute,
    )

    mod = _load_backfill()
    asyncio.run(mod.cmd_apply(ns))


if __name__ == "__main__":
    main()
