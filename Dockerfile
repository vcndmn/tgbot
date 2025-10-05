# Use a slim Python base
FROM python:3.11-slim

# Python runtime settings
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# App mode: "server" or "bot"
ENV APP_MODE=server
# Entrypoints (override if your files live in a subfolder)
ENV SERVER_ENTRY=server.py
ENV BOT_ENTRY=tgot.py

# OS deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates tini \ && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 10001 appuser
WORKDIR /app

# --- Dependencies ---
# Expect requirements.txt in the *build context root*, i.e., next to this Dockerfile.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \    && pip install --no-cache-dir \       telethon \       "python-telegram-bot>=21,<22"

# --- App code ---
COPY . /app/
RUN chown -R appuser:appuser /app

# Switch to non-root
USER appuser

# Expose HTTP port (server mode)
EXPOSE 8080

# Healthcheck (no-op in bot mode)
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \ CMD [ \"/bin/sh\", \"-lc\", \"[ \\\"$APP_MODE\\\" = \\\"server\\\" ] && curl -fsS http://localhost:8080/api/status >/dev/null || exit 0\"]

# Proper signal handling
ENTRYPOINT ["/usr/bin/tini", "--"]

# Choose what to run based on APP_MODE
CMD ["/bin/sh", "-lc", "if [ \"$APP_MODE\" = \"bot\" ]; then python ${BOT_ENTRY}; else python ${SERVER_ENTRY} ${PORT:-8080}; fi"]
