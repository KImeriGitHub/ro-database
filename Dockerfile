# python:3.11-slim-bookworm pins Python 3.11 + Debian bookworm. For full
# build reproducibility, append @sha256:<digest> after the tag -- look one
# up with:
#   docker pull python:3.11-slim-bookworm && \
#   docker inspect --format='{{index .RepoDigests 0}}' python:3.11-slim-bookworm
# and commit the resulting reference here.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies as root (system site-packages), then drop privileges
# for the source-copy and runtime steps.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Non-root runtime user. UID/GID 1001 avoids collision with the base image's
# reserved system ids.
RUN groupadd --system --gid 1001 app \
 && useradd --system --uid 1001 --gid app --no-create-home --shell /usr/sbin/nologin app \
 && chown -R app:app /app

# Copy source. secrets/ is excluded via .dockerignore; the container reads
# GCP credentials via ADC and Alpha Vantage keys from Secret Manager.
# historical_data_setup/ is included because daily/weekend runs import
# helpers from historical_data_setup/_common.py; the FirstRate CSV path
# itself is not exercised inside the container.
COPY --chown=app:app config/ ./config/
COPY --chown=app:app maintainance_scripts/ ./maintainance_scripts/
COPY --chown=app:app asset_catalog_service/ ./asset_catalog_service/
COPY --chown=app:app historical_data_setup/ ./historical_data_setup/
COPY --chown=app:app daily_data_service/ ./daily_data_service/
COPY --chown=app:app monitoring_service/ ./monitoring_service/
COPY --chown=app:app scheduled_scripts/ ./scheduled_scripts/

USER app

# ENTRYPOINT + CMD split so the runtime module can be swapped at container
# start without re-stating the python interpreter (default runs the daily
# ingest; override CMD to run a different module).
ENTRYPOINT ["python"]
CMD ["-m", "scheduled_scripts.run_daily"]
