package ru.urfu.cake.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "cake.kafka.streams")
public class KafkaStreamsProperties {

    private String applicationId;
    private String bootstrapServers;
    private int numStreamThreads = 1;
    private String stateDir = "/tmp/kafka-streams";
    private boolean topologyLogging = false;

    private Retry retry = new Retry();
    private Dlq dlq = new Dlq();
    private Metrics metrics = new Metrics();

    @Setter
    @Getter
    public static class Retry {
        private int maxAttempts = 3;
        private long backoffMs = 1000;

    }

    @Setter
    @Getter
    public static class Dlq {
        private boolean enable = true;
        private String topicSuffix = ".DLQ";

    }

    @Setter
    @Getter
    public static class Metrics {
        private boolean enable = true;

    }
}