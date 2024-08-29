package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.jooq.tools.json.JSONObject;
import org.jooq.tools.json.JSONParser;
import org.jooq.tools.json.ParseException;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeSet;

@Slf4j
@Builder
public class ProcessorOfRule implements RuleProcessor {
    ClientOfRedis clientOfRedis;

    @Override
    public Message processing(Message message, Rule[] rules) throws ParseException {
        log.debug("MASSAGE:" + message.getValue());
        String messageValue = message.getValue();
        messageValue = messageValue.replace(":,", ":null,");
        messageValue = messageValue.replace(":-,", ":null,");
        JSONObject jsonObject = (JSONObject) (new JSONParser().parse(messageValue));
        boolean allFieldsContains = true;
        TreeSet<String> keys = new TreeSet<>();
        StringBuilder key = new StringBuilder();
        message.setDeduplicationState(false);
        for (Rule rule : rules) {
            log.debug("RULE:" + rule);
            if (Boolean.TRUE.equals(rule.getIsActive())) {
                if (jsonObject.containsKey(rule.getFieldName())) {
                    String jsonValue = (jsonObject.get(rule.getFieldName()) == null) ? ("null") : jsonObject.get(rule.getFieldName()).toString();
                    key.append("|").append(rule.getFieldName()).append(":").append(jsonValue).append("|");
                    keys.add(key.toString());
                    key = new StringBuilder();
                    message.setDeduplicationState(addToRedis(Arrays.toString(keys.toArray()), rule.getTimeToLiveSec()));
                } else {
                    allFieldsContains = false;
                }
            }
        }
        if (!allFieldsContains) {
            message.setDeduplicationState(false);
        }
        return message;
    }

    boolean addToRedis(String key, Long timeToLiveSec) {
        log.debug("addToRedis_KEY:" + key);
        boolean isInRedis = clientOfRedis.isKeyInRedis(key);
        log.debug("addToRedis_KEY:" + key + "TIME:" + timeToLiveSec + "IN_REDIS:" + isInRedis);
        if (!isInRedis) {
            clientOfRedis.writeData(key);
            clientOfRedis.expire(key, timeToLiveSec);
        }
        return isInRedis;
    }
}
