package ru.mai.lessons.rpks.impl;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.impl.settings.MongoDBSettings;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;

@Slf4j
@Builder
public class ClientOfMongoDB implements MongoDBClientEnricher {
    MongoDBSettings mongoDBSettings; //_id -max

    Document readFromMongoDB(Rule rule) {
        log.debug("RULE" + rule.toString());
        try (var mongoClient = MongoClients.create(mongoDBSettings.getConnectionString())) {
            log.debug("CONNECT_TO_MONGO:");
            MongoDatabase mongoDatabase = mongoClient.getDatabase(mongoDBSettings.getDatabase());
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(mongoDBSettings.getCollection());
            log.debug("COLLECTION: {}", mongoCollection);
            return mongoCollection.find(new BasicDBObject(rule.getFieldNameEnrichment(), rule.getFieldValue()))
                    .sort(new Document("_id", -1)).first();
        }
    }
}
