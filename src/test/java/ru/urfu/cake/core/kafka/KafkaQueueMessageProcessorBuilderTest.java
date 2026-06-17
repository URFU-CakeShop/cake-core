package ru.urfu.cake.core.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import jdk.jfr.Description;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.urfu.cake.core.process.DeadLetterPublisher;
import ru.urfu.cake.core.queue.QueueMessageConfig;
import ru.urfu.cake.core.queue.QueueMessageHandler;

import java.util.Properties;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
/**
 * Модульные тесты к билдеру обработчиков очередей
 */
public class KafkaQueueMessageProcessorBuilderTest {
    private static final String TEST_TOPIC = "test-topic";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final DeadLetterPublisher deadLetterPublisher = mock();
    private final QueueMessageHandler<TestMessage> handler = mock();
    private TopologyTestDriver driver;
    private final QueueMessageConfig<TestMessage> config = new QueueMessageConfig<>() {
        @Override
        public String getTopicName() {
            return TEST_TOPIC;
        }
        @Override
        public String serialize(TestMessage message) {
            try {
                return OBJECT_MAPPER.writeValueAsString(message);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        @Override
        public TestMessage deserialize(String str) {
            try {
                return OBJECT_MAPPER.readValue(str, TestMessage.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        @Override
        public String resolveMessageId(TestMessage message) {
            return message.value();
        }
    };
    /**
     * Тестовое сообщение
     *
     * @param value Содержимое сообщения
     */
    public record TestMessage(String value) {}

    @BeforeEach
    void setUp() {
        var builder = new StreamsBuilder();
        var processorBuilder = new KafkaQueueMessageProcessorBuilder(builder, deadLetterPublisher);
        processorBuilder.configure(config, handler);

        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(builder.build(), props);
    }
    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }
    @Test
    @Description("Тест на обработку валидного сообщения")
    void processValidMessageTest() throws Exception {
        var inputTopic = createInputTopic();
        var message = new TestMessage("hello");
        inputTopic.pipeInput("key1", "{\"value\":\"hello\"}");
        verify(handler).handle(message);
        verify(deadLetterPublisher, never()).publish(anyString(), anyString(), anyString(), any(), any());
    }
    @Test
    @Description("Тест на отправку в DLQ при ошибке обработчика")
    void sendToDlqOnHandlerExceptionTest() throws Exception {
        doThrow(new RuntimeException("processing error")).when(handler).handle(any());
        var inputTopic = createInputTopic();
        inputTopic.pipeInput("key1", "{\"value\":\"fail\"}");
        verify(handler).handle(any());
        verify(deadLetterPublisher).publish(eq(TEST_TOPIC), eq("key1"), anyString(), isNull(), any());
    }
    private TestInputTopic<String, String> createInputTopic() {
        return driver.createInputTopic(
            TEST_TOPIC,
            Serdes.String().serializer(),
            Serdes.String().serializer()
        );
    }
}
