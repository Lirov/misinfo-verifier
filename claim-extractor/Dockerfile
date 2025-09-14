FROM python:3.11-slim
WORKDIR /app
COPY common/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY claim-extractor/app app
CMD ["python", "app/worker.py"]
