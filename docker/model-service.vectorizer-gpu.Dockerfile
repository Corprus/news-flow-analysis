# syntax=docker/dockerfile:1.6

ARG PYTORCH_IMAGE=mirror.gcr.io/pytorch/pytorch:2.7.1-cuda12.8-cudnn9-runtime
FROM ${PYTORCH_IMAGE}

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src
ENV HF_HOME=/app/.cache/huggingface
ENV HF_HUB_DISABLE_XET=1

COPY requirements-model-service.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY data/artifacts/models/final_exp10 /app/data/artifacts/models/final_exp10

EXPOSE 8000

CMD ["uvicorn", "model_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
