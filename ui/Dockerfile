FROM python:3.12-slim

WORKDIR /app

COPY ui/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ui /app

EXPOSE 8501

CMD ["sh", "-c", "streamlit run app.py --server.address=0.0.0.0 --server.port=8501 --server.maxUploadSize=${NEWS_IMPORT_MAX_FILE_MIB:-512}"]
