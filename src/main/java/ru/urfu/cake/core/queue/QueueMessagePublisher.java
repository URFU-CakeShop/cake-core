package ru.urfu.cake.core.queue;
/**
 * Публикатор сообщений
 *
 * @param <T> Тип сообщения
 */
public interface QueueMessagePublisher<T> {
    /**
     * Опубликовать сообщение
     *
     * @param message Сообщение
     */
    void publish(T message);
}
