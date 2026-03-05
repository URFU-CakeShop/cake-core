package ru.urfu.cake.handler;

import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.urfu.cake.dlq.DeadLetterPublisher;


import java.util.Map;

@Setter
public class LoggingDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingDeserializationExceptionHandler.class);

    private DeadLetterPublisher deadLetterPublisher;

    public LoggingDeserializationExceptionHandler() {}

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Deserialization error topic={} partition={} offset={}", record.topic(), record.partition(), record.offset(), exception);
        if (deadLetterPublisher != null && record.value() != null) {
            String value = new String(record.value());
            String key = record.key() != null ? new String(record.key()) : null;
            deadLetterPublisher.publish(record.topic(), key, value, record.headers(), exception);
        }
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {}
}