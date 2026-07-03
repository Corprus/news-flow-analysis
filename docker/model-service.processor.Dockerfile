# syntax=docker/dockerfile:1.6

FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

RUN apt-get update \
    && apt-get install --no-install-recommends -y libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-model-processor.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY data/artifacts/models/final_exp10 /app/data/artifacts/models/final_exp10

EXPOSE 8000

CMD ["uvicorn", "model_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
