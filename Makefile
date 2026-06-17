run:
	go run ./cmd/server

kafka-topic:
	docker-compose exec kafka kafka-topics \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic car-commands \
		--partitions 1 --replication-factor 1