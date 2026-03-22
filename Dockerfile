# Стадия deps: здесь допустимы HTTP_PROXY/HTTPS_PROXY / зеркало PyPI только для pip.
# Финальный образ их не наследует; TELEGRAM_HTTP_PROXY — только в рантайме (env_file).
FROM python:3.12-slim AS deps

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_DEFAULT_TIMEOUT=180

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ENV HTTP_PROXY=$HTTP_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
ENV NO_PROXY=$NO_PROXY

# Свой индекс PyPI (оба должны быть заданы)
ARG PIP_INDEX_URL=
ARG PIP_TRUSTED_HOST=
# Или одно значение: 1 / true → зеркало Aliyun (часто быстрее, если pypi.org таймаутит)
ARG PIP_USE_MIRROR=

WORKDIR /tmp
COPY requirements.txt .

# Без set -x: иначе в логе путаются квадратные скобки из echo и из "if ["
RUN set -e; \
    echo "deps: pip install begin"; \
    if [ -n "${PIP_INDEX_URL}" ] && [ -n "${PIP_TRUSTED_HOST}" ]; then \
      pip install --no-cache-dir --retries 20 --timeout 180 \
        -i "${PIP_INDEX_URL}" --trusted-host "${PIP_TRUSTED_HOST}" \
        -r requirements.txt; \
    elif [ "${PIP_USE_MIRROR}" = "1" ] || [ "${PIP_USE_MIRROR}" = "true" ]; then \
      echo "deps: using Aliyun PyPI mirror"; \
      pip install --no-cache-dir --retries 20 --timeout 180 \
        -i https://mirrors.aliyun.com/pypi/simple/ \
        --trusted-host mirrors.aliyun.com \
        -r requirements.txt; \
    else \
      echo "deps: using pypi.org"; \
      pip install --no-cache-dir --retries 20 --timeout 180 -r requirements.txt; \
    fi; \
    echo "deps: pip install end"

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=deps /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=deps /usr/local/bin /usr/local/bin

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
