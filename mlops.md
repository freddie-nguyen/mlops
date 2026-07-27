# MLOPs

### Đề bài: Hệ thống phát hiện bất thường (anomaly detection) cho metrics hạ tầng, phục vụ realtime

**Bối cảnh:** Các dịch vụ cloud phát ra metrics liên tục (CPU, RAM, disk, request latency, error rate...). Xây một hệ thống nhận luồng metrics realtime, dùng ML model phát hiện điểm bất thường, và cảnh báo - kèm đầy đủ vòng đời MLOps.

lộ trình:

**Tuần 1:** làm quen data + train model baseline. dựng Kafka producer/consumer, sinh metrics giả.

⇒ output: code base train được dữ liệu đơn giản. kafka hoạt động ổn định serving được lượng dữ liệu đủ lớn để training model? 10/50/100M message

**Tuần 2:** Wrap model thành inference service.

⇒ chuẩn hóa model thành service chạy độc lập với input là metrics

**Tuần 3:** Nối inference vào luồng Kafka realtime.

⇒ output: sử dụng dữ liệu realtime của server được bắn lên kafka và detach được sự bất thường từ metrics

**Tuần 4:** Containerize + deploy.

⇒ output: chuẩn hóa inference service thành container (tối ưu về dung lượng image)

**Tuần 5:** CI/CD pipeline + model registry (MLflow). Auto-deploy model mới.

**Tuần 6:** Monitoring + drift detection + alert (Prometheus/Grafana/Evidently).

⇒ output cuối cùng: một hệ thống chạy được với dữ liệu realtime nhận từ kafka và có cảnh báo gửi về (email/telegram,…) khi phát hiện có điểm bất thường + một tài liệu thiết kế

> hard hơn: thêm auto-retraining - khi drift vượt ngưỡng, tự trigger retrain qua pipeline
> 

Gợi ý:

- hệ thống phân làm 2 luồng: vận hành và ML lifecycle (train/deploy/retrain)
    - luồng vận hành realtime (từ khi metrics được sinh ra cho tới khi trở thành cảnh báo)
        
        !image.png
        
        - Diễn giải luồng vận hành: metrics phát ra liên tục được đẩy vào Kafka topic. Inference service (consumer group) đọc từ topic, đưa từng điểm/window vào model để tính điểm bất thường, so với ngưỡng. Nếu vượt ngưỡng → sinh alert (lưu vào DB làm lịch sử + gửi notification). Song song, mọi kết quả inference được đẩy sang monitoring để theo dõi latency, throughput và **drift** (khi phân bố metrics thay đổi, báo hiệu model cần train lại).
    - **luồng ML lifecycle** - cách model được train, đóng gói, deploy và tự cập nhật
        
        !image.png
        
        - Diễn giải luồng lifecycle: train model trên data lịch sử (metrics đã gán nhãn bình thường/bất thường), đánh giá bằng precision/recall. Model tốt được đăng ký vào registry (MLflow) kèm version. CI/CD tự build image inference chứa model version mới rồi deploy. Khi hệ thống monitoring phát hiện drift, nó trigger retrain - vòng lặp khép kín quay lại bước train.

### thứ tự bắt đầu

1. Dựng Kafka + DB + Grafana, verify từng cái sống, hoạt động và sử dụng được.
2. Viết producer giả sinh metrics (các thông số như cpu, ram, disk, network) đẩy vào Kafka (chưa cần data thật).
3. Viết consumer đọc message → lưu DB → dựng dashboard Grafana đọc từ DB. **Lúc này pipeline đã chạy end-to-end mà chưa có model** - dùng rule ngưỡng cứng (ví dụ CPU > 90% = alert) để test luồng alert.
4. Thay rule cứng bằng model đã train.