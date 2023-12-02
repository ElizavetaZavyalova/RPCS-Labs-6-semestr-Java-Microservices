package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.impl.settings.ConsumerSettings;
import ru.mai.lessons.rpks.model.Message;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Getter
@Setter
@Builder
public class ReaderFromKafka implements KafkaReader {
    private AtomicBoolean isExit;
    private ConsumerSettings consumerSettings;
    WriterToKafka writerToKafka;

    @Override
    public void processing() {
        assert writerToKafka != null;
        writerToKafka.createKafkaProducer();
        log.debug("CONSUMER_SETTINGS:" + consumerSettings);
        log.debug("KAFKA_CONSUMER_START_READING_FROM_TOPIC {}", consumerSettings.getTopicIn());
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerSettings.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, consumerSettings.getGroupId(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerSettings.getAutoOffsetReset()
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
        kafkaConsumer.subscribe(Collections.singletonList(consumerSettings.getTopicIn()));
        try (kafkaConsumer) {
            while (!isExit.get()) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.debug("MASSAGE_FROM_KAFKA_TOPIC {} : {}", consumerRecord.topic(), consumerRecord.value());
                    writerToKafka.processing(Message.builder().value(consumerRecord.value()).build());
                }
            }
            log.debug("READ_IS_DONE!");
        }
    }
}
