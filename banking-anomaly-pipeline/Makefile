.PHONY: up down logs restart clean topics

up:
	docker compose up --build -d
	@echo "Dashboard → http://localhost:8080"

down:
	docker compose down

logs:
	docker compose logs -f producer processor dashboard

restart-processor:
	docker compose restart processor

topics:
	docker exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list

tail-alerts:
	docker exec kafka kafka-console-consumer.sh \
	  --bootstrap-server kafka:9092 \
	  --topic fraud-alerts \
	  --from-beginning \
	  --formatter kafka.tools.DefaultMessageFormatter \
	  --property print.key=true

clean:
	docker compose down -v --remove-orphans
