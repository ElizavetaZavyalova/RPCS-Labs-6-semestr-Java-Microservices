package ru.mai.lessons.rpks.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.impl.settings.DBSettings;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

@Setter
@Slf4j
@Builder
@Getter
public class ReaderFromDB implements DbReader {
    DBSettings dbSettings;

    @Override
    public Rule[] readRulesFromDB() {
        try (HikariDataSource hikariDataSource = new HikariDataSource(makeHikariConfig())) {
            log.debug("HIKARY_CREATE");
            DSLContext context = DSL.using(hikariDataSource.getConnection(), SQLDialect.POSTGRES);
            log.debug("CONTEXT_MADE");
            Result<Record> information = context.select().from(dbSettings.getTableName()).fetch();
            Rule[] rules = new Rule[information.size()];
            log.debug("RULLES_CREATE:" + information.size());
            int currentRuleIndex = 0;
            for (Record ruleInformation : information) {
                rules[currentRuleIndex] = Rule.builder().filterId((Long) ruleInformation.get("filter_id"))
                        .ruleId((Long) ruleInformation.get("rule_id"))
                        .fieldName((String) ruleInformation.get("field_name"))
                        .filterFunctionName((String) ruleInformation.get("filter_function_name"))
                        .filterValue((String) ruleInformation.get("filter_value")).build();
                log.debug("FIND_RULE:" + rules[currentRuleIndex].toString());
                currentRuleIndex++;
            }
            log.debug("MAKE_RULES_FROM_DB");
            return rules;
        } catch (SQLException e) {
            log.debug("SQLException " + e.getMessage());
        }
        return new Rule[0];
    }


    HikariConfig makeHikariConfig() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(dbSettings.getJdbcUrl());
        hikariConfig.setUsername(dbSettings.getUser());
        hikariConfig.setPassword(dbSettings.getPassword());
        hikariConfig.setDriverClassName(dbSettings.getDriver());
        log.debug("HIKARY_CREATE");
        return hikariConfig;
    }
}
