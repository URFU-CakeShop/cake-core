package ru.urfu.cake.core.model;

import lombok.Data;

import java.util.Map;
import java.util.UUID;
/**
 * Задача на отправку уведомления.
 */
@Data
public class NotificationTask {
    private UUID userId;
    private String type;
    private String status;
    private Map<String, Object> payload;
    private String to;
    private String subject;
    private String htmlContent;
}
