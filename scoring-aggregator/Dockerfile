FROM python:3.11-slim
WORKDIR /app
COPY common/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scoring-aggregator/app app
CMD ["python", "-m", "app.worker"]
