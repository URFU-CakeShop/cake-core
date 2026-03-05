package ru.urfu.cake.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

public record KafkaStreamsTopologyLogger(StreamsBuilderFactoryBean factoryBean)
        implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaStreamsTopologyLogger.class);

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {

        if (factoryBean.getTopology() != null) {

            log.info(
                    "Kafka Streams topology:\n{}",
                    factoryBean.getTopology().describe()
            );
        }
    }
}