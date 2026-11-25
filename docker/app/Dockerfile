# Python-сервисы платформы: SHOP API, CLIENT API, CLI списка запрещённых товаров,
# потоковый фильтр на Faust и приёмник вебхуков Alertmanager.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY app/requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY app/marketplace ./marketplace

CMD ["python", "-m", "marketplace.moderation", "worker", "-l", "info"]
