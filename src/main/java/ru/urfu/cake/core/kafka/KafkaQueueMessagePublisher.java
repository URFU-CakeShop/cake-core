package ru.urfu.cake.core.kafka;

import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import ru.urfu.cake.core.queue.QueueMessagePublisher;
import ru.urfu.cake.core.queue.QueueMessageConfig;
/**
 * Реализация {@link QueueMessagePublisher} на базе Apache Kafka
 *
 * @param <T> Типизация сообщения
 */
@AllArgsConstructor
public class KafkaQueueMessagePublisher<T> implements QueueMessagePublisher<T> {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final QueueMessageConfig<T> config;
    /**
     * Опубликовать сообщение
     *
     * @param message Сообщение
     */
    @Override
    public void publish(T message) {
        try {
            var topic = config.getTopicName();
            var json = config.serialize(message);
            var key = config.resolveMessageId(message);
            kafkaTemplate.send(topic, key, json);
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception", e);
        }
    }
}
