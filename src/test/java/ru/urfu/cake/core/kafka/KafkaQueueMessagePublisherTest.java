package ru.urfu.cake.core.kafka;

import jdk.jfr.Description;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import ru.urfu.cake.core.queue.QueueMessageConfig;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
/**
 * Модульные тесты к публикатору сообщений
 */
public class KafkaQueueMessagePublisherTest {
    private final KafkaTemplate<String, String> kafkaTemplate = mock();
    private final QueueMessageConfig<String> config = mock();
    private final KafkaQueueMessagePublisher<String> publisher = new KafkaQueueMessagePublisher<>(kafkaTemplate, config);
    @Test
    @Description("Тест на корректную отправку сообщения в топик")
    void sendMessageTest() {
        var message = "test-message";
        var topic = "test-topic";
        when(config.getTopicName()).thenReturn(topic);
        when(config.serialize(message)).thenReturn("serialized-json");
        when(config.resolveMessageId(message)).thenReturn("test-key");
        publisher.publish(message);
        verify(config).getTopicName();
        verify(config).serialize(message);
        verify(config).resolveMessageId(message);
        verify(kafkaTemplate).send(topic, "test-key", "serialized-json");
    }
    @Test
    @Description("Тест на ")
    void wrapErrorsDuringPublishTest() {
        var message = "fail-message";
        when(config.serialize(message)).thenThrow(new RuntimeException("serialization failed"));
        assertThrows(RuntimeException.class, () -> publisher.publish(message));
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }
}
