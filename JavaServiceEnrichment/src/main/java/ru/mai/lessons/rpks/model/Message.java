package ru.mai.lessons.rpks.model;

import lombok.*;

@Getter
@Setter
@ToString
@Builder
public class Message {
    private String value; // сообщение из Kafka в формате JSON
}
