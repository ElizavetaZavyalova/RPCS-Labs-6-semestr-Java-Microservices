package ru.mai.lessons.rpks.impl.settings;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@ToString
public class ConsumerSettings {
    private String bootstrapServers;
    private String groupId;
    private String autoOffsetReset;
    private String topicIn;
    private int updateIntervalSec;
}
