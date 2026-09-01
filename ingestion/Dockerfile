FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Default entry point is the main ingestion consumer; the retry service
# (Phase 9) uses the same image with its command overridden in
# docker-compose.yml to run app.retry_main instead.
CMD ["python", "-m", "app.main"]
