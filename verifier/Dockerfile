FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY common/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY verifier/requirements.txt verifier-requirements.txt
RUN pip install --no-cache-dir -r verifier-requirements.txt
COPY verifier/app app
CMD ["python", "-m", "app.worker"]
