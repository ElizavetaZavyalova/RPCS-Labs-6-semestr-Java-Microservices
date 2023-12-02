package ru.mai.lessons.rpks.impl;

import lombok.extern.slf4j.Slf4j;
import org.jooq.tools.json.JSONObject;
import org.jooq.tools.json.JSONParser;
import org.jooq.tools.json.ParseException;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class ProcessorOfRule implements RuleProcessor {
    @Override
    public Message processing(Message message, Rule[] rules) throws ParseException {
        String messageValue = message.getValue();
        messageValue = messageValue.replace(":,", ":null,");
        messageValue = messageValue.replace(":-,", ":null,");
        JSONObject jsonObject = (JSONObject) (new JSONParser().parse(messageValue));
        message.setFilterState(false);
        for (Rule rule : rules) {
            if (!isSatisfiedTheRule(jsonObject, rule)) {
                return message;
            }
        }
        if (rules.length > 0) {
            message.setFilterState(true);
        }
        return message;
    }

    enum FUNCTION_NAME {
        EQUALS, CONTAINS, NOT_EQUALS, NOT_CONTAINS
    }

    private boolean isSatisfiedTheRule(JSONObject jsonObject, Rule rule) {
        String jsonValue = "null";
        if (jsonObject.containsKey(rule.getFieldName())) {
            jsonValue = (jsonObject.get(rule.getFieldName()) == null) ? ("null") : jsonObject.get(rule.getFieldName()).toString();
        }
        if (jsonValue.equals("null")) {
            return false;
        }
        log.info("jsonValueOf" + rule.getFieldName() + ": " + jsonValue);
        switch (FUNCTION_NAME.valueOf(rule.getFilterFunctionName().toUpperCase())) {
            case EQUALS -> {
                return isEquals(jsonValue, rule.getFilterValue());
            }//equals, contains, not_equals, not_contains
            case CONTAINS -> {
                return isContains(jsonValue, rule.getFilterValue());
            }
            case NOT_EQUALS -> {
                return isNotEquals(jsonValue, rule.getFilterValue());
            }
            case NOT_CONTAINS -> {
                return isNotContains(jsonValue, rule.getFilterValue());
            }
            default -> {
                log.warn("NOT_CORRECT_FILTER_FUNCTION_NAME" + rule.getFilterFunctionName());
                return true;
            }
        }
    }

    boolean isEquals(String jsonValue, String filterValue) {
        log.debug("EQUALS:");
        return jsonValue.equals(filterValue);
    }

    boolean isContains(String jsonValue, String filterValue) {
        log.debug("CONTAINS:");
        return jsonValue.contains(filterValue);
    }

    boolean isNotEquals(String jsonValue, String filterValue) {
        log.debug("NOT_EQUALS:");
        return !isEquals(jsonValue, filterValue);
    }

    boolean isNotContains(String jsonValue, String filterValue) {
        log.debug("NOT_CONTAINS:");
        return !isContains(jsonValue, filterValue);
    }
}
