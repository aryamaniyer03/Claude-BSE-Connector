FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better caching
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e .

# Expose port (Railway sets PORT env var)
EXPOSE 8000

# Run the HTTP server
CMD ["bse-connector-http"]
