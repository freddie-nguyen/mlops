# stage 1
FROM python:3.11-slim AS builder
## set working directory là /build (Debian Linux của base image) 
WORKDIR /build
## copy file requirements.txt từ build context sang thư mục hiện tại làm việc của image (/base) (annotated by dot)
COPY requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# stage 2
FROM python:3.11-slim

WORKDIR /app
## copy thư viện cài từ stage 1 (builder) (được lưu trong /install) → sang stage 2 (lưu trong /user/local) 
COPY --from=builder /install /usr/local

COPY app.py .

COPY models/ ./models/

RUN useradd -m appuser
USER appuser

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]