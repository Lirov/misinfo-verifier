FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY common/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY retriever/app app
CMD ["python", "-u", "-m", "app.worker"]
