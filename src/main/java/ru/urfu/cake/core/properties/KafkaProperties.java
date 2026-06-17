package ru.urfu.cake.core.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
/**
 * Свойства Kafka Stream
 */
@Setter
@Getter
@ConfigurationProperties(prefix = "cake.kafka.streams")
public class KafkaProperties {
    private String applicationId;
    private String bootstrapServers;
    private int numStreamThreads = 1;
    private String stateDir = "/tmp/kafka-streams";
    private Retry retry = new Retry();
    private Dlq dlq = new Dlq();
    private Metrics metrics = new Metrics();
    /**
     * Повторные попытки
     */
    @Setter
    @Getter
    public static class Retry {
        private int maxAttempts = 3;
        private long backoffMs = 1000;
    }
    /**
     * Dead Letter Exchange
     */
    @Setter
    @Getter
    public static class Dlq {
        private boolean enable = true;
        private String topicSuffix = ".DLQ";
    }
    /**
     * Метрики
     */
    @Setter
    @Getter
    public static class Metrics {
        private boolean enable = true;
    }
}