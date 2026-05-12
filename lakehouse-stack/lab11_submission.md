# LAB 11 — CHANGE DATA CAPTURE (CDC) WITH DEBEZIUM: Submission

This document contains the terminal outputs and answers for the Lab 11 CDC assignment using Debezium and PostgreSQL.

## 1. Screenshot of docker ps

```text
$ docker ps | grep -E "postgres|connect|kafka|zookeeper"
53fa28414fd3   debezium/connect:2.4              "/docker-entrypoint.…"   Up 14 minutes   0.0.0.0:8083->8083/tcp, [::]:8083->8083/tcp, 9092/tcp   de_connect
af715c0a3f87   postgres:15                       "docker-entrypoint.s…"   Up 14 minutes   0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp             de_postgres
15c69f548d82   confluentinc/cp-kafka:7.5.0       "/etc/confluent/dock…"   Up 47 minutes   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp             kafka
1a9e3cd5c698   confluentinc/cp-zookeeper:7.5.0   "/etc/confluent/dock…"   Up 47 minutes   0.0.0.0:2181->2181/tcp, [::]:2181->2181/tcp             zookeeper
```

## 2. POST connector success

```text
$ curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @pg-orders-connector.json

{"name":"pg-orders-connector","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.hostname":"de_postgres","database.port":"5432","database.user":"de_user","database.password":"de_password","database.dbname":"de_db","database.server.name":"app","table.include.list":"public.orders","topic.prefix":"app","slot.name":"orders_slot","publication.name":"dbz_publication","plugin.name":"pgoutput","name":"pg-orders-connector"},"tasks":[],"type":"source"}
```

## 3. Consume topic có event insert / update / delete

*(Note: Data fields like schema are omitted/truncated for readability)*

### Create Event (INSERT) `op = "c"`
```json
{
  "payload": {
    "before": null,
    "after": { "id": 1, "customer_name": "Alice", "status": "created", "amount": "LxI=" },
    "source": { "connector": "postgresql", "db": "de_db", "table": "orders", "lsn": 26861352 },
    "op": "c",
    "ts_ms": 1775148505849
  }
}
```

### Update Event (UPDATE) `op = "u"`
```json
{
  "payload": {
    "before": { "id": 1, "customer_name": "Alice", "status": "created", "amount": "LxI=" },
    "after": { "id": 1, "customer_name": "Alice", "status": "paid", "amount": "LxI=" },
    "source": { "connector": "postgresql", "db": "de_db", "table": "orders", "lsn": 26861616 },
    "op": "u",
    "ts_ms": 1775148505851
  }
}
```

### Delete Event (DELETE) `op = "d"`
```json
{
  "payload": {
    "before": { "id": 1, "customer_name": "Alice", "status": "paid", "amount": "LxI=" },
    "after": null,
    "source": { "connector": "postgresql", "db": "de_db", "table": "orders", "lsn": 26861736 },
    "op": "d",
    "ts_ms": 1775148505851
  }
}
```

## 4. Câu hỏi ngắn

**CDC khác batch ETL ở đâu?**
CDC (Change Data Capture) bắt và truyền tải các sự kiện thay đổi dữ liệu (INSERT, UPDATE, DELETE) ngay khi chúng xảy ra (near real-time), giúp đồng bộ dữ liệu liên tục với độ trễ thấp. Ngược lại, Batch ETL trích xuất và tải một lượng lớn dữ liệu theo định kỳ (nightly, weekly), có độ trễ cao hơn. Hơn nữa, CDC giảm thiểu việc tải lên database do chỉ đọc log.

**Vì sao CDC giảm tải database hơn scan toàn bảng?**
CDC (với Debezium) đọc nội dung trực tiếp từ Write-Ahead Log (WAL) mà database tự sinh ra trong các giao dịch. Việc đọc log replication ở tầng cấu trúc vật lý giúp source system lược bỏ hoàn toàn các chi phí xử lý câu query (đọc I/O table data) nên ít ảnh hưởng đến hiệu năng đang phục vụ của source DB. Quét toàn bảng (scan) sẽ lock table hoặc gây I/O cực cao khiến nghẽn hệ thống.

**WAL đóng vai trò gì?**
WAL (Write-Ahead Log) là tệp nhật ký ghi lại tất cả các thay đổi trước khi dữ liệu được chốt vào đĩa lưu trữ chính (commit). Vai trò cơ bản của WAL là đảm bảo khả năng phục hồi dữ liệu khi có sự cố. Trong CDC, WAL đóng vai trò làm "nguồn phát" cho các event; Debezium sẽ theo dõi và đọc chuỗi WAL này để parsing thành JSON stream liên tục.

**Debezium event chứa before / after / op để làm gì?**
- `before`: Chứa trạng thái của dòng dữ liệu trước khi thay đổi được áp dụng. Điều này quan trọng khi UPDATE/DELETE để hệ thống downstream biết record nào/value cũ nào cần vô hiệu hóa hoặc để tính toán chênh lệch (delta). (Với INSERT `before` luôn bằng null).
- `after`: Chứa trạng thái của dữ liệu sau thay đổi. Được hệ thống target sử dụng khi lưu trữ giá trị hiện tại (UPSERT). (Với DELETE `after` luôn bằng null).
- `op`: Operation flag giúp xác định loại thao tác (`c` = create, `u` = update, `d` = delete). Nhờ cờ hiệu này mà downstream app sẽ thực thi logic phù hợp như `INSERT`, `UPDATE` hay `DELETE` tương ứng vào data warehouse.
