import logging
import os
from logging.handlers import TimedRotatingFileHandler

from config import LOG_DIR, LOG_RETENTION_DAYS


def setup_logging(service_name: str) -> None:
    """
    Настройка логирования:
    - stdout (для docker logs)
    - файл с ротацией раз в день и хранением LOG_RETENTION_DAYS файлов.
    """
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Не дублируем хендлеры при повторном вызове.
    if root.handlers:
        return

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    root.addHandler(stream_handler)

    os.makedirs(LOG_DIR, exist_ok=True)
    file_path = os.path.join(LOG_DIR, f"{service_name}.log")
    file_handler = TimedRotatingFileHandler(
        filename=file_path,
        when="midnight",
        interval=1,
        backupCount=LOG_RETENTION_DAYS,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

