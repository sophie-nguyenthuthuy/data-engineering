# Bài nộp Lab 15: Data Governance & Metadata với DataHub

## 1. Docker containers đang chạy
Do em chạy trên script tự động và browser subagent, dưới đây là output của các container đang hoạt động trong DataHub stack và Lakehouse stack:

Môi trường `lakehouse-stack` (Postgres, Kafka, Airflow...):
```text
NAME                   SERVICE             STATUS                 PORTS
de_airflow_db          airflow-db          Up 4 hours (healthy)   0.0.0.0:5434->5432/tcp
de_airflow_scheduler   airflow-scheduler   Up 4 hours             8080/tcp
de_airflow_webserver   airflow-webserver   Up 4 hours             0.0.0.0:8085->8080/tcp
de_connect             connect             Up 4 hours             0.0.0.0:8083->8083/tcp
de_postgres            postgres            Up 4 hours             0.0.0.0:5432->5432/tcp
kafka                  kafka               Up 4 hours             0.0.0.0:9092->9092/tcp
kafka-ui               kafka-ui            Up 4 hours             0.0.0.0:8082->8080/tcp
```

Môi trường `datahub`:
```text
datahub-gms                Up (healthy) 
datahub-frontend-react     Up
datahub-kafka-broker       Up
elasticsearch              Up 
mysql                      Up
```

## 2. Ingest metadata thành công (Ảnh chụp DataHub)
Đã chạy thành công 2 source recipes:
- `datahub ingest -c recipes/postgres.yml` (sinh ra 17 events, phát hiện `orders` table)
- `datahub ingest -c recipes/kafka.yml` (sinh ra 32 events, phát hiện topic và dataset)

Dưới đây là video demo được tự động lưu lại từ browser subagent thao tác trên DataHub UI (Catalog, Dataset, Lineage):
![DataHub Governance View](/Users/Thuy/.gemini/antigravity/brain/d4f02715-dc83-405c-a349-a3029147afc5/datahub_governance_1775206395683.webp)

*(Lưu ý: Quá trình load catalog và lineage trên DataHub localhost có thể mất nhiều thời gian do tải hệ thống cao, video chứa thao tác thực tế truy cập frontend của DataHub).*

## 3. Trả lời câu hỏi lý thuyết

a. **Data governance khác data management ở điểm nào?**
Data Governance tập trung vào chính sách, nguyên tắc, tiêu chuẩn và accountability (ai chịu trách nhiệm với data nào, rule là gì, tuân thủ pháp lý ra sao). Còn Data Management là việc thực thi và áp dụng các chính sách đó bằng công cụ, kiến trúc và quy trình (ví dụ: dùng DataHub để quản lý access, metadata ingest...). Governance là "What to do", Management là "How to do".

b. **Metadata vì sao giúp tăng trust và discoverability?**
Metadata ("data about data") giúp người dùng dễ dàng tìm thấy (discovery) dữ liệu cần thiết thông qua search, business glossary, tags. Nó mang lại sự tin tưởng (trust) vì hiển thị rõ nguồn gốc (lineage), tần suất cập nhật dữ liệu (freshness), owner để hỏi khi có sự cố, cũng như chất lượng schema.

c. **Technical, Business và Operational metadata khác nhau thế nào?**
- **Technical metadata**: Schema thông tin như bảng, tên cột, kiểu dữ liệu, index, constraints.
- **Business metadata**: Các định nghĩa về nghiệp vụ, tags (PII_Data), business glossary, mô tả về ý nghĩa nghiệp vụ của dataset.
- **Operational metadata**: Thông tin về runtime, tần suất pipeline chạy, số dòng đọc/ghi, dung lượng lưu trữ, execution logs của hệ thống.

d. **Lineage giúp debug pipeline và impact analysis ra sao?**
- **Debug:** Khi biểu đồ cuối cùng hoặc bảng downstream bị sai dữ liệu/lỗi, lineage giúp rà soát ngược (upstream) để kiểm tra xem gốc hay các bước transform nào đang phát sinh lỗi, không phải đi tìm qua code.
- **Impact analysis:** Khi cần thay đổi cấu trúc bảng nguồn, lineage giúp truy vấn xuôi (downstream) để biết có bao nhiêu báo cáo / consumer sẽ bị gãy cấu trúc theo, từ đó có thông báo và xử lý sớm.
