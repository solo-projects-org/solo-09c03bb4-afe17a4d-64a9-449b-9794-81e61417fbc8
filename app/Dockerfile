FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

# Samples and the published count matrix are stored here (see DATA_DIR).
RUN mkdir -p /data
VOLUME /data

# Single instance: annotation batch state is in memory, so run one worker so
# every request sees the same registry, the same sample store and one matrix
# rebuild loop.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
