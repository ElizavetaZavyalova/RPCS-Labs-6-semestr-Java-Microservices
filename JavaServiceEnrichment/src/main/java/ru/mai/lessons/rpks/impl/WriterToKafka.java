package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jooq.tools.json.ParseException;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.impl.settings.ProducerSettings;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Getter
@Setter
@Builder
public class WriterToKafka implements KafkaWriter {
    ProducerSettings producerSettings;
    ConcurrentLinkedQueue<Rule[]> rules;
    ProcessorOfRule processorOfRule;
    KafkaProducer<String, String> kafkaProducer;

    @Override
    public void processing(Message message) {
        assert kafkaProducer != null;
        try {
            while (rules.isEmpty()) {
                log.debug("EMPTY_RULES");
            }
            processorOfRule.processing(message, rules.peek());
            kafkaProducer.send(new ProducerRecord<>(producerSettings.getTopicOut(), message.getValue()));
        } catch (ParseException e) {
            log.warn("NOT_CORRECT_MASSAGE:" + message.getValue());
        }
    }

    void createKafkaProducer() {
        assert producerSettings != null;
        kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, producerSettings.getBootstrapServers(),
                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
    }
}

