package ru.urfu.cake.core.queue;
/**
 * Обработчик сообщений
 *
 * @param <T> Тип сообщения
 */
public interface QueueMessageHandler<T> {
    /**
     * Обработать сообщение
     *
     * @param message Сообщение
     */
    void handle(T message) throws Exception;
}
