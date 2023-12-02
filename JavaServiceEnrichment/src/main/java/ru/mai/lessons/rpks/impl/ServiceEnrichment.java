package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.impl.settings.ConsumerSettings;
import ru.mai.lessons.rpks.impl.settings.DBSettings;
import ru.mai.lessons.rpks.impl.settings.MongoDBSettings;
import ru.mai.lessons.rpks.impl.settings.ProducerSettings;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ServiceEnrichment implements Service {
    @Override
    public void start(Config config) {
        // написать код реализации сервиса обогащения
        log.debug("CONFIG:" + config.toString());
        AtomicBoolean isExit = new AtomicBoolean(false);
        ConcurrentLinkedQueue<Rule[]> rules = new ConcurrentLinkedQueue<>();
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        ConsumerSettings consumerSettings = Settings.makeConsumerSettings(config);
        ProducerSettings producerSettings = Settings.makeProducerSettings(config);
        DBSettings dbSettings = Settings.makeDBSettings(config);
        MongoDBSettings mongoDBSettings = Settings.makeMongoDBSettings(config);
        int updateIntervalSec = config.getConfig("application").getInt("updateIntervalSec");


        ClientOfMongoDB clientOfMongoDB = ClientOfMongoDB.builder().mongoDBSettings(mongoDBSettings).build();
        ProcessorOfRule processorOfRule = ProcessorOfRule.builder().clientOfMongoDB(clientOfMongoDB).build();

        WriterToKafka writerToKafka = WriterToKafka.builder().producerSettings(producerSettings).rules(rules)
                .processorOfRule(processorOfRule).build();
        ReaderFromKafka readerFromKafka = ReaderFromKafka.builder().consumerSettings(consumerSettings)
                .isExit(isExit).writerToKafka(writerToKafka).build();

        executorService.submit(readerFromKafka::processing);
        ReaderFromDB readerFromDB = ReaderFromDB.builder().dbSettings(dbSettings).build();
        while (!isExit.get()) {
            rules.add(readerFromDB.readRulesFromDB());
            log.debug("ADD_RULE");
            if (rules.size() > 1) {
                rules.poll();
            }
            try {
                Thread.sleep(updateIntervalSec * 1000L);
            } catch (InterruptedException e) {
                log.warn("CANT_SLEEP:" + e.getMessage());
                Thread.currentThread().interrupt();
                break;
            }
        }
        executorService.shutdown();
    }

    private record Settings() {
        private static MongoDBSettings makeMongoDBSettings(Config config) {
            Config mongoDBConfig = config.getConfig("mongo");
            MongoDBSettings mongoDBSettings = MongoDBSettings.builder()
                    .connectionString(mongoDBConfig.getString("connectionString"))
                    .database(mongoDBConfig.getString("database"))
                    .collection(mongoDBConfig.getString("collection")).build();
            log.debug("MONGO_DB_SETTINGS_WAS_READ: " + mongoDBSettings.toString());
            return mongoDBSettings;
        }

        private static ConsumerSettings makeConsumerSettings(Config config) {
            Config kafkaConfigConsumer = config.getConfig("kafka").getConfig("consumer");
            ConsumerSettings consumerSettings = ConsumerSettings.builder()
                    .groupId(kafkaConfigConsumer.getString("group.id"))
                    .bootstrapServers(kafkaConfigConsumer.getString("bootstrap.servers"))
                    .autoOffsetReset(kafkaConfigConsumer.getString("auto.offset.reset"))
                    .topicIn(kafkaConfigConsumer.getString("topicIn")).build();
            log.debug("CONSUMER_SETTINGS_WAS_READ: " + consumerSettings.toString());
            return consumerSettings;
        }

        private static ProducerSettings makeProducerSettings(Config config) {
            Config kafkaConfigProducer = config.getConfig("kafka").getConfig("producer");
            ProducerSettings producerSettings = ProducerSettings.builder()
                    .bootstrapServers(kafkaConfigProducer.getString("bootstrap.servers"))
                    .topicOut(kafkaConfigProducer.getString("topicOut")).build();
            log.debug("PRODUCER_SETTINGS_WAS_READ: " + producerSettings.toString());
            return producerSettings;
        }

        private static DBSettings makeDBSettings(Config config) {
            Config dbConfig = config.getConfig("db");
            DBSettings dbSettings = DBSettings.builder().jdbcUrl(dbConfig.getString("jdbcUrl"))
                    .driver(dbConfig.getString("driver"))
                    .user(dbConfig.getString("user"))
                    .password(dbConfig.getString("password"))
                    .tableName(dbConfig.getString("tableName"))
                    .enrichmentId(config.getInt("application.enrichmentId")).build();
            log.debug("DB_SETTINGS_WAS_READ: " + dbSettings.toString());
            return dbSettings;
        }
    }
}
