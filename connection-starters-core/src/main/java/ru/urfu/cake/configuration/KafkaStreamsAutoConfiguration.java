package ru.urfu.cake.configuration;

import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import ru.urfu.cake.dlq.DeadLetterPublisher;
import ru.urfu.cake.handler.LoggingDeserializationExceptionHandler;
import ru.urfu.cake.properties.KafkaStreamsProperties;

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