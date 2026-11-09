FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 ANO_DATA_DIR=/var/data/ano
RUN useradd --uid 10001 --create-home ano && mkdir -p /var/data/ano && chown -R ano:ano /var/data/ano
RUN pip install --no-cache-dir gunicorn==23.0.0
WORKDIR /app
COPY ano_worker /app/ano_worker
USER ano
CMD ["python", "-m", "ano_worker.serve"]
