package com.nannoq.tools.repository.repository;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

import java.util.function.Consumer;

/**
 * This class contains logic for get and performing retryable redis commands.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class RedisUtils {
    private static final Logger logger = LoggerFactory.getLogger(RedisUtils.class.getSimpleName());

    public static RedisClient getRedisClient(Vertx vertx, JsonObject config) {
        String redisServer = config.getString("redis_host");
        RedisOptions redisOptions = new RedisOptions();
        redisOptions.setHost(redisServer);

        if (redisServer != null && redisServer.equals("localhost")) redisOptions.setPort(6380);

        return RedisClient.create(vertx, redisOptions);
    }

    public static void performJedisWithRetry(RedisClient redisClient,
                                             Consumer<RedisClient> consumer) {
        consumer.accept(redisClient);
    }
}
