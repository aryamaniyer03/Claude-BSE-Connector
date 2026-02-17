FROM python:3.11-slim

# Render sets PORT env var — default to 10000
ENV PORT=10000 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first for better caching
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e .

# Create writable cache directory inside the container
RUN mkdir -p /app/cache && chmod 777 /app/cache
ENV BSE_CACHE_DIR=/app/cache

# Non-root user for security
RUN useradd --create-home appuser
USER appuser

EXPOSE ${PORT}

# Use exec form so uvicorn receives SIGTERM directly for graceful shutdown
CMD ["bse-connector-http"]
