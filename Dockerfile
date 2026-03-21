FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    WEB_PORT=8182 \
    THEME_DB_PATH=/app/instance/theme_library.db

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /app/instance

EXPOSE 8182

CMD ["python", "app.py"]
