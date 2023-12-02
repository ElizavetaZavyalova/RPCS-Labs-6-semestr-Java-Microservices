package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.jooq.tools.json.JSONObject;
import org.jooq.tools.json.JSONParser;
import org.jooq.tools.json.ParseException;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Objects;

@Slf4j
@Builder
public class ProcessorOfRule implements RuleProcessor {
    ClientOfMongoDB clientOfMongoDB;

    @Override
    public Message processing(Message message, Rule[] rules) throws ParseException {
        for (Rule rule : rules) {
            setRules(message, rule);
        }
        return message;
    }

    void setRules(Message message, Rule rule) throws ParseException {
        JSONObject jsonObject = (JSONObject) (new JSONParser().parse(message.getValue()));
        Document findResult = clientOfMongoDB.readFromMongoDB(rule);
        if (Objects.nonNull(findResult)) {
            jsonObject.put(rule.getFieldName(), (new JSONParser().parse(findResult.toJson())));
        } else {
            jsonObject.put(rule.getFieldName(), rule.getFieldValueDefault().replace("\"", ""));
        }
        message.setValue(jsonObject.toString());
    }


}
