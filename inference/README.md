# Build và kiểm tra dung lượng

```
cd inference
```
Tạo docker image
Cờ -t: đặt tag cho image với version v1. (syntax: ```<tên_image>:<tag>```)
```
docker build -t anomaly-inference:v1 .
```
Hiển thị thông tin của Image vừa tạo
```
docker images anomaly-inference:v1
```
# Chạy thử container
```
docker run -d --name inference-test -p 8000:8000 anomaly-inference:v1
```
```
docker logs inference-test
```
```
curl.exe localhost:8000/health
```