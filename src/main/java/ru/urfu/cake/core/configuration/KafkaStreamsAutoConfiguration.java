package ru.urfu.cake.core.configuration;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import ru.urfu.cake.core.dlq.DeadLetterPublisher;
import ru.urfu.cake.core.handler.LoggingDeserializationExceptionHandler;
import ru.urfu.cake.core.properties.KafkaStreamsProperties;

import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
@EnableKafkaStreams
@EnableConfigurationProperties(KafkaStreamsProperties.class)
public class KafkaStreamsAutoConfiguration {

    @Bean(name = "defaultKafkaStreamsConfig")
    public KafkaStreamsConfiguration kafkaStreamsConfiguration(KafkaStreamsProperties properties) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LoggingDeserializationExceptionHandler.class);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public ProducerFactory<String, String> producerFactory(KafkaStreamsProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public DeadLetterPublisher deadLetterPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        return new DeadLetterPublisher(kafkaTemplate);
    }

    @Bean
    public KafkaStreamsTopologyLogger topologyLogger() {
        return new KafkaStreamsTopologyLogger(new StreamsBuilderFactoryBean());
    }
}