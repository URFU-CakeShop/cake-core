package ru.urfu.cake.core.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import ru.urfu.cake.core.queue.QueueMessageConfig;
/**
 * Конфигурация очереди уведомлений
 */
public class NotificationTaskQueueConfig implements QueueMessageConfig<NotificationTask> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    /**
     * Название топика
     */
    public static final String TOPIC_NAME = "notifications";
    /**
     * Получить название топика
     *
     * @return Наименование топика
     */
    @Override
    public String getTopicName() {
        return TOPIC_NAME;
    }
    /**
     * Сериализовать сообщение
     *
     * @param message Сообщение
     * @return Строка
     */
    @Override
    public String serialize(NotificationTask message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * Десериализовать сообщение
     *
     * @param str Строка
     * @return Сообщение
     */
    @Override
    public NotificationTask deserialize(String str) {
        try {
            return objectMapper.readValue(str, NotificationTask.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * Разрешить идентификатор сообщения
     *
     * @param message Сообщение
     * @return Идентификатор
     */
    @Override
    public String resolveMessageId(NotificationTask message) {
        return message.getUserId().toString();
    }
}
