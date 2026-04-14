FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .

# Cloud Run sets PORT; default to 8080 for GCR conventions
ENV PORT=8080

EXPOSE ${PORT}

CMD ["python", "server.py"]
