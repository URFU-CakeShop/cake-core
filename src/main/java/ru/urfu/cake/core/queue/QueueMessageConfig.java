package ru.urfu.cake.core.queue;
/**
 * Конфигурация
 *
 * @param <T> Типизация сообщения
 */
public interface QueueMessageConfig<T> {
    /**
     * Получить название топика
     *
     * @return Наименование топика
     */
    String getTopicName();
    /**
     * Сериализовать сообщение
     *
     * @param message Сообщение
     * @return Строка
     */
    String serialize(T message);
    /**
     * Десериализовать сообщение
     *
     * @param str Строка
     * @return Сообщение
     */
    T deserialize(String str);
    /**
     * Разрешить идентификатор сообщения
     *
     * @param message Сообщение
     * @return Идентификатор
     */
    String resolveMessageId(T message);
}
