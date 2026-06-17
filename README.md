# cake-core

Общая библиотека для Kafka Streams: сериализация, Dead Letter Queue, построение топологии и автоконфигурация.

## Тестирование

### Unit-тесты

Запускаются без дополнительной инфраструктуры:

```bash
./gradlew test
```

### Интеграционные тесты

Требуют запущенного экземпляра Kafka. Запускаются только с флагом `-Dintegration=true`.

**Запуск Kafka через Docker:**

```bash
docker run -d --name kafka-integration-test --network host \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENERS=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e CLUSTER_ID=test-cluster-id \
  apache/kafka:3.9.0
```

Дождаться готовности Kafka:

```bash
docker logs kafka-integration-test 2>&1 | grep "Kafka Server started"
```

Запустить тесты:

```bash
./gradlew test -Dintegration=true
```

Остановить Kafka:

```bash
docker rm -f kafka-integration-test
```

Bootstrap-сервер можно переопределить через `-Dkafka.bootstrap.servers=host:port` (по умолчанию `localhost:9092`).

### Состав тестов

| Тест | Тип | Что проверяет |
|---|---|---|
| `StreamTopologyFactoryTest` (3 теста) | unit | Обработка сообщений, DLQ при ошибке, фильтрация null |
| `KafkaStreamsIntegrationTest` (5 тестов) | integration | Dead Letter Queue, produce/consume, AdminClient, null-value, топология |
