# Lab 10 — Apache Kafka Fundamentals

## Objectives
- Hiểu broker / topic / partition / producer / consumer / consumer group.
- Publish và consume message bằng Kafka CLI.
- Quan sát partition assignment, offset, delivery semantics.

## Services bạn cần bật
```bash
docker compose up -d zookeeper kafka kafka-ui
```
Kiểm tra:
- `docker ps` — 3 container Up.
- Kafka UI: `http://localhost:8082` (cluster `local` tự xuất hiện).

## Bước 1 — Vào shell Kafka
```bash
docker exec -it kafka bash
```
Các lệnh bên dưới đều chạy **bên trong** container này.

## Bước 2 — Tạo topic `orders`
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic orders --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
```
Mong đợi: 3 partition, `ReplicationFactor: 1`.

Mở Kafka UI → Topics → `orders` để confirm.

## Bước 3 — Producer + Consumer (2 terminal riêng)

**Terminal A — producer:**
```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
>order_001,created
>order_002,paid
>order_003,shipped
>order_004,delivered
```

**Terminal B — consumer:**
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders --from-beginning
```
Mong đợi: 4 dòng message xuất hiện.

## Bước 4 — Keyed messages (cùng key → cùng partition)
```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders \
  --property "parse.key=true" --property "key.separator=:"
>customer_1:order_101_created
>customer_1:order_101_paid
>customer_2:order_202_created
```
Kiểm tra Kafka UI → `orders` → Messages → xem partition của mỗi message. Các message key `customer_1` phải đi cùng 1 partition.

## Bước 5 — Consumer group
Chạy 2 consumer với **cùng** `--group order-processors`:
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders --group order-processors
```
Các consumer trong cùng group chia partition với nhau.

Xem offsets / lag:
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group order-processors
```

## Deliverables
- Ảnh `docker ps` (kafka + zookeeper + kafka-ui).
- Ảnh Kafka UI showing topic `orders`.
- Ảnh terminal producer + consumer.
- Ảnh output `--describe --topic orders` và `--describe --group order-processors`.
- Trả lời 3 câu:
  1. Kafka dùng để làm gì?
  2. Partition để làm gì?
  3. Ordering được đảm bảo ở phạm vi nào?
- Khung submission: [`lab10_submission.md`](../lab10_submission.md).

## Self-check
- Delivery semantics: at-most-once / at-least-once / exactly-once — cái nào mặc định của Kafka + consumer tự commit?
- Nếu bạn cần đảm bảo toàn bộ event của 1 customer theo thứ tự, bạn phải thiết kế key thế nào?
- Thêm 1 consumer thứ 3 vào group với 3 partition — nó ảnh hưởng gì?
