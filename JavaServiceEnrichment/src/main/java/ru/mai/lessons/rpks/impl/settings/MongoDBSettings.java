package ru.mai.lessons.rpks.impl.settings;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@ToString
public class MongoDBSettings {
    String connectionString;
    String database;
    String collection;
}
