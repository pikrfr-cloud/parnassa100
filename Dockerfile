FROM python:3.12-slim

LABEL maintainer="Market Intel Bot"
LABEL description="Prediction market intelligence bot with Telegram alerts"

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create data directory for state persistence
RUN mkdir -p /data

# Health check — verify Python can import the bot
HEALTHCHECK --interval=300s --timeout=10s --retries=3 \
    CMD python -c "import main; print('ok')" || exit 1

# Run the bot
CMD ["python", "-u", "main.py"]
