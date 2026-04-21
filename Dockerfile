FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install only what the daily ingest path needs. The historical setup is not
# run inside the container, so FirstRate CSV handling deps are excluded.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source. secrets/ is excluded via .dockerignore; Cloud Run injects
# credentials via ADC and Secret Manager instead.
COPY config/ ./config/
COPY maintainance_scripts/ ./maintainance_scripts/
COPY asset_catalog_service/ ./asset_catalog_service/
COPY daily_data_service/ ./daily_data_service/
COPY scheduled_scripts/ ./scheduled_scripts/

ENTRYPOINT ["python", "-m", "scheduled_scripts.run_daily_ingest"]
