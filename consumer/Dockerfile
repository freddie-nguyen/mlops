# consumer/Dockerfile
FROM python:3.11-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /install /usr/local
COPY consumer.py .
RUN useradd -m appuser
USER appuser
ENV PYTHONUNBUFFERED=1
CMD ["python", "consumer.py"]