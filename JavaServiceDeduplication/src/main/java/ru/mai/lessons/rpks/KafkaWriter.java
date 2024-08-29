package ru.mai.lessons.rpks;

import ru.mai.lessons.rpks.model.Message;

public interface KafkaWriter {//Есть
    public void processing(Message message); // отправляет сообщения с deduplicationState = true в выходной топик. Конфигурация берется из файла *.conf
}
