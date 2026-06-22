FROM python:3.12-alpine

WORKDIR /app

RUN pip install --no-cache-dir docker==7.1.0 prometheus-client==0.22.1

COPY docker/metrics_exporter.py /app/metrics_exporter.py

EXPOSE 9101

CMD ["python", "/app/metrics_exporter.py"]
