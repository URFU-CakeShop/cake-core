package ru.urfu.cake.core.process;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DeadLetterPublisherTest {
    private final KafkaTemplate<String, String> kafkaTemplate = mock();
    private final DeadLetterPublisher publisher = new DeadLetterPublisher(kafkaTemplate);
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldSendToDlqTopic() {
        var sourceTopic = "source-topic";
        var key = "test-key";
        var value = "test-value";

        publisher.publish(sourceTopic, key, value, null, new RuntimeException("test error"));

        var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();

        assert record.topic().equals("source-topic.DLQ") : "Expected DLQ topic";
        assert record.key().equals("test-key") : "Expected key";
        assert record.value().equals("test-value") : "Expected value";
    }
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldIncludeHeaders() {
        var headers = new RecordHeaders();
        headers.add(new RecordHeader("trace-id", "abc-123".getBytes()));
        headers.add(new RecordHeader("source", "test-service".getBytes()));

        publisher.publish("source-topic", "key", "value", headers, new RuntimeException("test error"));

        var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();

        var traceHeader = record.headers().lastHeader("trace-id");
        var sourceHeader = record.headers().lastHeader("source");
        assert traceHeader != null && "abc-123".equals(new String(traceHeader.value())) : "Expected trace-id header";
        assert sourceHeader != null && "test-service".equals(new String(sourceHeader.value())) : "Expected source header";
    }
    @Test
    void shouldThrowStreamsExceptionOnKafkaFailure() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(new RuntimeException("kafka error"));
        assertThrows(StreamsException.class, () ->
            publisher.publish("source-topic", "key", "value", null, new RuntimeException("test error"))
        );
    }
}
