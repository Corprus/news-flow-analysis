# syntax=docker/dockerfile:1.6

FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src
ENV HF_HOME=/app/.cache/huggingface
ENV HF_HUB_DISABLE_XET=1

ARG PRELOAD_MODEL_FROM_HF=false
ARG HF_MODEL_ID=Korprus/news-flow-ru-vectorization-mpnet
ARG HF_MODEL_REVISION=789ab95331d9abc4f5f23d1e3d5d24bb8af28086
ARG HF_MODEL_DIR=/app/models/news-flow-ru-vectorization-mpnet/final
ARG HF_MODEL_METADATA_PATH=/app/configs/model_registry/latest_model.json

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY scripts/download_model.py /app/scripts/download_model.py
COPY configs/model_registry /app/configs/model_registry
RUN --mount=type=secret,id=hf_token,required=false \
    if [ "$PRELOAD_MODEL_FROM_HF" = "true" ]; then \
        export HF_TOKEN="$(cat /run/secrets/hf_token 2>/dev/null || true)" && \
        if [ -n "$HF_MODEL_METADATA_PATH" ]; then \
            python /app/scripts/download_model.py \
                --metadata-path "$HF_MODEL_METADATA_PATH" \
                --output-dir "$HF_MODEL_DIR"; \
        else \
            python /app/scripts/download_model.py \
                --repo-id "$HF_MODEL_ID" \
                --revision "$HF_MODEL_REVISION" \
                --output-dir "$HF_MODEL_DIR"; \
        fi; \
    fi

COPY src /app/src

EXPOSE 8000

CMD ["uvicorn", "model_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
