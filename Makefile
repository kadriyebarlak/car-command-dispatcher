.PHONY: run build check \
	kafka-topic \
	migrate-up migrate-down migrate-status \
	docker-up docker-down docker-stop \
	prometheus-up prometheus-down prometheus-logs \
	monitoring-up

DB_URL := postgres://notify:notify@localhost:5432/car_commands?sslmode=disable

run:
	go run ./cmd/server

build:
	go build -o bin/server ./cmd/server

check:
	go build ./...

kafka-topic:
	docker-compose exec kafka kafka-topics \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic car-commands \
		--partitions 1 --replication-factor 1

migrate-up:
	goose -dir migrations postgres "$(DB_URL)" up

migrate-down:
	goose -dir migrations postgres "$(DB_URL)" down

migrate-status:
	goose -dir migrations postgres "$(DB_URL)" status

docker-up:
	docker-compose up -d postgres kafka

docker-stop:
	docker-compose stop postgres kafka

docker-down:
	docker-compose down

prometheus-up:
	docker-compose up -d prometheus

prometheus-down:
	docker-compose stop prometheus

prometheus-logs:
	docker-compose logs -f prometheus

monitoring-up:
	docker-compose up -d prometheus