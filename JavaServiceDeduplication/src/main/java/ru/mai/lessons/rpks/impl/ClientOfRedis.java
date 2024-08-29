package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.JedisPubSub;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.impl.settings.RedisSettings;

import java.util.Optional;

@Slf4j
@Builder
public class ClientOfRedis implements RedisClient {
    RedisSettings redisSettings;
    private JedisPooled jedis;

    public void writeData(String key) {
        getJedis().set(key, "");
        log.debug("WRITE_DATE_TO_REDIS: {}", key);
    }

    public boolean isKeyInRedis(String findKey) {
        return getJedis().exists(findKey);
    }

    public void expire(String key, long seconds) {
        getJedis().expire(key, seconds);
        log.debug("SET_TIME_TO_LIVE {} SECONDS_BY_KEY {}", seconds, key);

    }

    private JedisPooled getJedis() {
        return Optional.ofNullable(jedis).orElse(new JedisPooled(redisSettings.getHost(), redisSettings.getPort()));
    }
}
