package ru.urfu.cake.core.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import ru.urfu.cake.core.process.DeadLetterPublisher;
import ru.urfu.cake.core.queue.QueueMessageConfig;
import ru.urfu.cake.core.queue.QueueMessageHandler;
/**
 * Билдер обработчиков очередей Apache Kafka
 */
@Slf4j
public class KafkaQueueMessageProcessorBuilder {
    private final StreamsBuilder builder;
    private final DeadLetterPublisher deadLetterPublisher;
    /**
     * Конструктор
     *
     * @param builder Билдер потоков Apache Kafka
     * @param deadLetterPublisher Очередь мёртвых сообщений
     */
    public KafkaQueueMessageProcessorBuilder(
        StreamsBuilder builder,
        DeadLetterPublisher deadLetterPublisher
    ) {
        this.builder = builder;
        this.deadLetterPublisher = deadLetterPublisher;
    }
    /**
     * Сконфигурировать обработчик сообщений
     *
     * @param config Конфигурация очереди
     * @param handler Обработчик сообщений
     * @param <T> Типизация сообщения
     */
    public <T> void configure(QueueMessageConfig<T> config, QueueMessageHandler<T> handler) {
        var topic = config.getTopicName();
        builder
            .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
            .filter((key, value) -> {
                if (value == null) {
                    log.warn("Received null message key={} topic={} — skipping", key, topic);
                    return false;
                }
                return true;
            })
            .foreach((key, value) -> {
                try {
                    var message = config.deserialize(value);
                    handler.handle(message);
                } catch (Exception e) {
                    log.error("Error processing message key={} topic={}", key, topic, e);
                    deadLetterPublisher.publish(topic, key, value, null, e);
                }
            });
    }
}
