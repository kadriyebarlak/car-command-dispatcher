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

DB_URL=postgres://notify:notify@localhost:5432/car_commands?sslmode=disable

migrate-up:
	goose -dir migrations postgres "$(DB_URL)" up

migrate-down:
	goose -dir migrations postgres "$(DB_URL)" down

migrate-status:
	goose -dir migrations postgres "$(DB_URL)" status

docker-up:
	docker-compose up -d postgres kafka

docker-down:
	docker-compose down postgres kafka