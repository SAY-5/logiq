FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY logiq ./logiq
RUN pip install --no-cache-dir .

EXPOSE 8000
ENV LOGIQ_DB=/data/logiq.db
ENV LOGIQ_WAL=/data/logiq.wal
VOLUME ["/data"]

CMD ["logiq", "serve", "--db", "/data/logiq.db", "--wal", "/data/logiq.wal", "--port", "8000"]
