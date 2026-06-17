package ru.urfu.cake.core.kafka;

import lombok.AllArgsConstructor;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import ru.urfu.cake.core.process.DeadLetterPublisher;
import ru.urfu.cake.core.properties.KafkaProperties;

import java.util.HashMap;
import java.util.Map;
/**
 * Автоконфигурация Kafka Streams для микросервисов.
 */
@AutoConfiguration
@EnableKafkaStreams
@EnableConfigurationProperties(KafkaProperties.class)
@AllArgsConstructor
public class KafkaConfiguration {
    private final KafkaProperties properties;
    /**
     * Конфигурация Kafka Streams
     */
    @Bean
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, KafkaLoggingDeserializationExceptionHandler.class);
        return new KafkaStreamsConfiguration(props);
    }

    /**
     * Шаблон для отправки сообщений в Kafka
     *
     * @return Шаблон Kafka
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }
    /**
     * Логирование десериализации сообщения
     */
    @Setter
    static public class KafkaLoggingDeserializationExceptionHandler implements DeserializationExceptionHandler {
        private static final Logger log = LoggerFactory.getLogger(KafkaLoggingDeserializationExceptionHandler.class);
        private DeadLetterPublisher deadLetterPublisher;

        /**
         * Обрабатывает ошибку десериализации: логирует и отправляет сообщение в DLQ.
         *
         * @param context   Контекст обработчика
         * @param record    Запись, которую не удалось десериализовать
         * @param exception Исключение при десериализации
         * @return CONTINUE — пропустить сообщение и продолжить обработку
         */
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

        /**
         * Конфигурация обработчика — в текущей реализации не используется.
         *
         * @param configs Карта конфигурационных параметров
         */
        @Override
        public void configure(Map<String, ?> configs) {}
    }
}
