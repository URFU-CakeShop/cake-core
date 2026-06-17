package ru.urfu.cake.core.process;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
/**
 * Публикует сообщения в Dead Letter Queue при ошибках обработки.
 */
@Component
public class DeadLetterPublisher {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    /**
     * Конструктор
     *
     * @param kafkaTemplate Шаблон для отправки сообщений в Kafka
     */
    public DeadLetterPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    /**
     * Публикует сообщение в DLQ-топик.
     *
     * @param topic       исходный топик
     * @param key         ключ сообщения
     * @param value       значение сообщения
     * @param headers     заголовки
     * @param exception   ошибка, которая произошла
     */
    public void publish(String topic, String key, String value, Headers headers, Exception exception) {
        String dlqTopic = topic + ".DLQ";
        log.error("Sending message to DLQ topic={} key={} due to exception={}", dlqTopic, key, exception.getMessage());
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(dlqTopic, key, value);
            if (headers != null) {
                headers.forEach(header -> record.headers().add(header));
            }
            kafkaTemplate.send(record);
        } catch (Exception e) {
            log.error("Failed to send message to DLQ", e);
            throw new StreamsException("DLQ publishing failed", e);
        }
    }
}