#!/usr/bin/env python3
"""
Массовая простановка ответственного по сделкам, где сейчас указан пользователь интеграции
(например «бизнесавтоматизатор»), по owner из МойСклад — та же логика, что у поллера.

Упрощённый запуск с обязательным --execute: scripts/apply_leads_execute.py

Примеры:
  # Найти numeric id пользователя amo по имени/email
  python scripts/backfill_integration_leads.py list-users

  # Отчёт: только «активные» (не успех/провал), без PATCH
  python scripts/backfill_integration_leads.py report --user-id 12345678

  # Только сделки, обновлённые с начала сегодня (МСК) — «что не успело обработаться»
  python scripts/backfill_integration_leads.py report --user-id 12345678 --today-moscow

  # Реально проставить ответственных (осторожно: много запросов к API)
  python scripts/backfill_integration_leads.py apply --user-id 12345678 --execute

В .env можно задать AMO_INTEGRATION_RESPONSIBLE_USER_ID — тогда --user-id не обязателен.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _setup_env() -> None:
    import os

    os.chdir(ROOT)
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")


REASON_RU: Dict[str, str] = {
    "ok": "Ответственный проставлен из МойСклад (apply) или готов к проставлению (report / dry-run)",
    "ignored_no_moysklad_order_link": "Нет заполненного поля «Ссылка на заказ в МойСклад» — интеграция такие сделки не обрабатывает",
    "no_counterparty_in_moysklad": "Контрагент в МойСклад не найден по ИНН компании / телефону",
    "no_owner_in_moysklad": "Контрагент в МойСклад без поля owner",
    "no_user_mapping": "В БД нет соответствия user_mapping (owner МС → пользователь amo)",
    "error": "Ошибка запроса / исключение при обработке",
}


def _msk_start_of_today_unix() -> int:
    from zoneinfo import ZoneInfo

    msk = ZoneInfo("Europe/Moscow")
    now = datetime.now(msk)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp())


def _fmt_ts(ts: int) -> str:
    if not ts:
        return "-"
    from zoneinfo import ZoneInfo

    return datetime.fromtimestamp(ts, tz=ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d %H:%M MSK")


async def _cmd_list_users() -> None:
    _setup_env()
    from amocrm_client import list_users

    users = await list_users()
    users.sort(key=lambda u: int(u.get("id") or 0))
    print(f"{'id':>12}  {'email':<40}  name")
    print("-" * 90)
    for u in users:
        uid = u.get("id")
        email = (u.get("email") or "")[:40]
        name = (u.get("name") or "").replace("\n", " ")[:60]
        print(f"{uid!s:>12}  {email:<40}  {name}")


def _resolve_user_id(arg: Optional[int]) -> int:
    _setup_env()
    from config import AMO_INTEGRATION_RESPONSIBLE_USER_ID

    if arg is not None:
        return int(arg)
    if AMO_INTEGRATION_RESPONSIBLE_USER_ID is not None:
        return int(AMO_INTEGRATION_RESPONSIBLE_USER_ID)
    raise SystemExit(
        "Укажите --user-id или задайте AMO_INTEGRATION_RESPONSIBLE_USER_ID в .env "
        "(см. list-users)."
    )


async def _filter_leads(
    user_id: int,
    *,
    active_only: bool,
    updated_from_unix: Optional[int],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    from amocrm_client import (
        fetch_leads_by_responsible_user,
        get_leads_pipelines,
        terminal_status_ids_from_pipelines,
    )

    pipelines = await get_leads_pipelines()
    closed_ids = terminal_status_ids_from_pipelines(pipelines)
    raw = await fetch_leads_by_responsible_user(user_id)

    out: List[Dict[str, Any]] = []
    for L in raw:
        try:
            sid = int(L.get("status_id") or 0)
        except (TypeError, ValueError):
            sid = 0
        if active_only and sid in closed_ids:
            continue
        ts = int(L.get("updated_at") or 0)
        if updated_from_unix is not None and ts < updated_from_unix:
            continue
        out.append(L)

    meta = {
        "total_on_user": len(raw),
        "after_filters": len(out),
        "closed_status_ids_count": len(closed_ids),
    }
    return out, meta


async def _process_leads(
    leads: List[Dict[str, Any]],
    *,
    dry_run: bool,
    max_leads: Optional[int] = None,
) -> List[Tuple[int, str, int, Dict[str, Any], Optional[str]]]:
    from amo_add_lead_sync import process_amo_add_lead_owner_sync

    rows: List[Tuple[int, str, int, Dict[str, Any], Optional[str]]] = []
    todo = leads if max_leads is None else leads[: max(0, int(max_leads))]
    for L in todo:
        lid = int(L["id"])
        name = (L.get("name") or "")[:55].replace("\n", " ")
        uat = int(L.get("updated_at") or 0)
        err: Optional[str] = None
        try:
            result = await process_amo_add_lead_owner_sync(
                lid,
                source="backfill_script",
                source_ip="backfill_integration_leads.py",
                dry_run=dry_run,
            )
        except Exception as exc:  # noqa: BLE001
            result = {"status": "error"}
            err = f"{type(exc).__name__}: {exc}"
        rows.append((lid, name, uat, result, err))
    return rows


def _print_summary(rows: List[Tuple[int, str, int, Dict[str, Any], Optional[str]]]) -> Counter:
    c: Counter = Counter()
    for _lid, _name, _uat, res, err in rows:
        st = res.get("status", "?")
        if err:
            st = "error"
        c[st] += 1
    print("\n=== Сводка по статусам ===")
    for k, v in c.most_common():
        label = REASON_RU.get(str(k), k)
        print(f"  {v:4}  {k}: {label}")
    return c


def _print_today_failures(
    rows: List[Tuple[int, str, int, Dict[str, Any], Optional[str]]],
    today_from: int,
) -> None:
    bad = [
        r
        for r in rows
        if r[2] >= today_from and (r[4] or r[3].get("status") not in ("ok",))
    ]
    # «Необработанные» = обновлены сегодня, но не dry-run ok
    unprocessed = [
        r for r in rows if r[2] >= today_from and r[3].get("status") != "ok" and not r[4]
    ]
    errors = [r for r in rows if r[2] >= today_from and r[4]]

    print("\n=== За сегодня (МСК, с полуночи) ===")
    print(f"  Сделок в выборке с updated_at сегодня: {sum(1 for r in rows if r[2] >= today_from)}")
    print(f"  Не получится проставить ответственного (по правилам интеграции): {len(unprocessed)}")
    print(f"  Ошибки API при проверке: {len(errors)}")

    if unprocessed:
        print("\n  Причины (сегодня, без ошибок):")
        sub = Counter(r[3].get("status", "?") for r in unprocessed)
        for st, cnt in sub.most_common():
            print(f"    {cnt} × {st}: {REASON_RU.get(st, st)}")
        print("\n  lead_id | updated_at        | статус")
        for lid, name, uat, res, err in unprocessed[:40]:
            print(f"  {lid} | {_fmt_ts(uat)} | {res.get('status')} | {name[:40]}")
        if len(unprocessed) > 40:
            print(f"  ... ещё {len(unprocessed) - 40}")

    if errors:
        print("\n  Ошибки:")
        for lid, name, uat, res, err in errors[:15]:
            print(f"  {lid} | {_fmt_ts(uat)} | {err}")


async def cmd_report(args: argparse.Namespace) -> None:
    _setup_env()
    from config import AMO_LEADS_POLL_DATE_FIELD, AMO_LEADS_POLL_INTERVAL_SECONDS

    user_id = _resolve_user_id(args.user_id)
    updated_from = _msk_start_of_today_unix() if args.today_moscow else None
    if args.updated_from_unix is not None:
        updated_from = int(args.updated_from_unix)

    leads, meta = await _filter_leads(
        user_id,
        active_only=not args.include_closed,
        updated_from_unix=updated_from,
    )

    print(f"Ответственный user_id={user_id}")
    print(f"Всего сделок на этом ответственном (API): {meta['total_on_user']}")
    print(f"После фильтров (активные={not args.include_closed}, updated_from={updated_from}): {meta['after_filters']}")
    if args.max_leads is not None:
        print(f"Ограничение --max-leads={args.max_leads} (для полного отчёта уберите флаг)")
    print()

    rows = await _process_leads(leads, dry_run=True, max_leads=args.max_leads)
    for lid, name, uat, res, err in rows:
        st = res.get("status", "?")
        extra = ""
        if st == "ok" and res.get("would_amocrm_user_id"):
            extra = f" → amo user {res.get('would_amocrm_user_id')}"
        if err:
            st = f"ERROR: {err}"
        print(f"{lid:12}  {_fmt_ts(uat):20}  {st:40}{extra}  {name}")

    _print_summary(rows)

    if args.today_moscow or args.updated_from_unix is not None:
        tf = updated_from if updated_from is not None else _msk_start_of_today_unix()
        _print_today_failures(rows, tf)

    print("\n--- Почему поллер мог «не увидеть» сделку сегодня ---")
    print(
        f"  Поллер использует фильтр по полю: {AMO_LEADS_POLL_DATE_FIELD} "
        f"(см. AMO_LEADS_POLL_DATE_FIELD в .env)."
    )
    print(
        "  Если стоит created_at — изменения по уже созданной сделке (только updated_at) "
        "в очередь не попадут, пока не сменится логика или не запустите этот скрипт."
    )
    print(f"  Интервал опроса: {AMO_LEADS_POLL_INTERVAL_SECONDS} с.")


async def cmd_apply(args: argparse.Namespace) -> None:
    if not args.execute:
        raise SystemExit("Для реальных изменений добавьте флаг --execute")

    _setup_env()
    user_id = _resolve_user_id(args.user_id)
    updated_from = _msk_start_of_today_unix() if args.today_moscow else None
    if args.updated_from_unix is not None:
        updated_from = int(args.updated_from_unix)

    leads, meta = await _filter_leads(
        user_id,
        active_only=not args.include_closed,
        updated_from_unix=updated_from,
    )
    print(
        f"APPLY: user_id={user_id}, сделок={meta['after_filters']}, "
        f"active_only={not args.include_closed}, updated_from={updated_from}"
    )

    rows = await _process_leads(leads, dry_run=False, max_leads=args.max_leads)
    _print_summary(rows)

    err_rows = [r for r in rows if r[4]]
    if err_rows:
        print("\n=== Сделки с исключением (см. текст ниже) ===")
        for lid, name, uat, _res, err in err_rows:
            print(f"  lead_id={lid}  updated={_fmt_ts(uat)}  |  {err}")

    err_count = len(err_rows)
    ok_count = sum(1 for r in rows if not r[4] and r[3].get("status") == "ok")
    print(f"\nГотово: ok={ok_count}, ошибок={err_count}")


def main() -> None:
    p = argparse.ArgumentParser(description="Бэкфилл ответственного по сделкам интеграции (МойСклад → amo)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp_list = sub.add_parser("list-users", help="Список пользователей amo (id, email, имя)")
    sp_list.set_defaults(func=lambda a: asyncio.run(_cmd_list_users()))

    sp_rep = sub.add_parser("report", help="Dry-run: диагностика без изменений в amo")
    sp_rep.add_argument("--user-id", type=int, default=None, help="ID ответственного «интеграция» в amo")
    sp_rep.add_argument(
        "--include-closed",
        action="store_true",
        help="Учитывать закрытые (успех/провал); по умолчанию только активные",
    )
    sp_rep.add_argument(
        "--today-moscow",
        action="store_true",
        help="Только сделки с updated_at с полуночи сегодня по Москве",
    )
    sp_rep.add_argument(
        "--updated-from-unix",
        type=int,
        default=None,
        help="Только сделки с updated_at >= unix timestamp",
    )
    sp_rep.add_argument(
        "--max-leads",
        type=int,
        default=None,
        help="Обработать только первые N сделок (ускорение теста)",
    )
    sp_rep.set_defaults(func=lambda a: asyncio.run(cmd_report(a)))

    sp_app = sub.add_parser("apply", help="Проставить ответственных (нужен --execute)")
    sp_app.add_argument("--user-id", type=int, default=None)
    sp_app.add_argument("--include-closed", action="store_true")
    sp_app.add_argument("--today-moscow", action="store_true")
    sp_app.add_argument("--updated-from-unix", type=int, default=None)
    sp_app.add_argument(
        "--execute",
        action="store_true",
        help="Без этого флага ничего не меняется",
    )
    sp_app.add_argument("--max-leads", type=int, default=None)
    sp_app.set_defaults(func=lambda a: asyncio.run(cmd_apply(a)))

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
