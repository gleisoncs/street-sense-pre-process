FROM python:3.11-slim

RUN pip install --no-cache-dir redis confluent-kafka

WORKDIR /app
COPY ddpai_adapter.py .

ENTRYPOINT ["python", "ddpai_adapter.py"]
