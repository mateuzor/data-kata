FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir spyne lxml wsgiref

COPY soap_server.py .

EXPOSE 8001

CMD ["python", "soap_server.py"]
