package ru.mai.lessons.rpks.impl.settings;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@ToString
public class DBSettings {
    private String jdbcUrl;
    private String user;
    private String password;
    private String driver;
    private String tableName;
    private Integer enrichmentId;
}
