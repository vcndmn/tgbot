FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1

ENV APP_MODE=server

RUN apt-get update && apt-get install -y --no-install-recommends     curl ca-certificates tini  && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 appuser
WORKDIR /app

COPY tgbot-main/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt     && pip install --no-cache-dir        telethon        "python-telegram-bot>=21,<22"

COPY tgbot-main/ /app/
RUN chown -R appuser:appuser /app

USER appuser
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3  CMD [ "$APP_MODE" = "server" ] && curl -fsS http://localhost:8080/api/status >/dev/null || exit 0

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/bin/sh", "-lc", "if [ "$APP_MODE" = "bot" ]; then python tgot.py; else python server.py ${PORT:-8080}; fi"]
