/*
 * MIT License
 *
 * Copyright (c) 2017 Anders Mikkelsen
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package com.nannoq.tools.repository.repository.etag;

import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.repository.cache.ClusterCacheManagerImpl;
import com.nannoq.tools.repository.repository.redis.RedisUtils;
import com.nannoq.tools.repository.utils.ItemList;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.nannoq.tools.repository.repository.redis.RedisUtils.performJedisWithRetry;

/**
 * The cachemanger contains the logic for setting, removing, and replace etags.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class RedisETagManagerImpl<E extends ETagable & Model> implements ETagManager<E> {
    private static final Logger logger = LoggerFactory.getLogger(RedisETagManagerImpl.class.getSimpleName());

    private final Class<E> TYPE;

    private final RedisClient REDIS_CLIENT;

    public RedisETagManagerImpl(Class<E> type, RedisClient redisClient) {
        TYPE = type;
        this.REDIS_CLIENT = redisClient;
    }

    public void removeProjectionsEtags(int hash, Handler<AsyncResult<Boolean>> resultHandler) {
        String etagKeyBase = TYPE.getSimpleName() + "_" + hash + "/projections";

        doEtagRemovalWithRetry(etagKeyBase, resultHandler);
    }

    public void destroyEtags(int hash, Handler<AsyncResult<Boolean>> resultHandler) {
        String etagItemListHashKey = TYPE.getSimpleName() + "_" + hash + "_" + "itemListEtags";

        doEtagRemovalWithRetry(etagItemListHashKey, resultHandler);
    }

    private void doEtagRemovalWithRetry(String etagKeyBase, Handler<AsyncResult<Boolean>> resultHandler) {
        performJedisWithRetry(REDIS_CLIENT, in -> in.hgetall(etagKeyBase, allRes -> {
            if (allRes.failed()) {
                resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
            } else {
                JsonObject result = allRes.result();
                List<String> itemsToRemove = new ArrayList<>();
                result.iterator().forEachRemaining(item -> itemsToRemove.add(item.getKey()));

                performJedisWithRetry(REDIS_CLIENT, inner -> inner.hdelMany(etagKeyBase, itemsToRemove, manyDelRes -> {
                    resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                }));
            }
        }));
    }

    public void replaceAggregationEtag(String etagItemListHashKey, String etagKey, String newEtag,
                                       Handler<AsyncResult<Boolean>> resultHandler) {
        performJedisWithRetry(REDIS_CLIENT, ir -> ir.hset(etagItemListHashKey, etagKey, newEtag, setRes -> {
            if (setRes.failed()) {
                resultHandler.handle(Future.failedFuture(setRes.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
            }
        }));
    }

    @Override
    public void setSingleRecordEtag(Map<String, String> etagMap,
                                    Handler<AsyncResult<Boolean>> resultHandler) {
        Consumer<RedisClient> consumer = redisClient -> etagMap.keySet()
                .forEach(key -> redisClient.set(key, etagMap.get(key), result -> {
                    if (result.failed()) {
                        logger.error("Could not set " + etagMap.get(key) + " for " + key + ",cause: " + result.cause());
                    }
                }));

        RedisUtils.performJedisWithRetry(REDIS_CLIENT, consumer);

        resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
    }

    @Override
    public void setProjectionEtags(String[] projections, int hash, E item) {
        if (projections != null && projections.length > 0 && item != null) {
            String etagKeyBase = TYPE.getSimpleName() + "_" + hash + "/projections";
            String key = TYPE.getSimpleName() + "_" + hash + "/projections" + Arrays.hashCode(projections);
            String etag = item.getEtag();

            RedisUtils.performJedisWithRetry(REDIS_CLIENT, in -> in.hset(etagKeyBase, key, etag, setRes -> {
                if (setRes.failed()) {
                    logger.error("Unable to store projection etag!", setRes.cause());
                }
            }));
        }
    }

    @Override
    public void setItemListEtags(String etagItemListHashKey, String etagKey, ItemList<E> itemList,
                                 Future<Boolean> itemListEtagFuture)  {
        RedisUtils.performJedisWithRetry(REDIS_CLIENT, in ->
                in.hset(etagItemListHashKey, etagKey, itemList.getEtag(), setRes -> {
                    itemListEtagFuture.complete(Boolean.TRUE);
                }));
    }

    @Override
    public void checkItemEtag(String etagKeyBase, String key, String etag,
                              Handler<AsyncResult<Boolean>> resultHandler) {
        RedisUtils.performJedisWithRetry(REDIS_CLIENT, innerRedis ->
                innerRedis.hget(etagKeyBase, key, getResult -> {
                    if (getResult.succeeded() && getResult.result() != null &&
                            getResult.result().equals(etag)) {
                        resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                    } else {
                        resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                    }
                }));
    }

    @Override
    public void checkItemListEtag(String etagItemListHashKey, String etagKey, String etag,
                                  Handler<AsyncResult<Boolean>> resultHandler) {
        RedisUtils.performJedisWithRetry(REDIS_CLIENT, innerRedis ->
                innerRedis.hget(etagItemListHashKey, etagKey, getResult -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Stored etag: " + getResult.result() + ", request: " + etag);
                    }

                    if (getResult.succeeded() && getResult.result() != null &&
                            getResult.result().equals(etag)) {
                        resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                    } else {
                        resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                    }
                }));
    }

    @Override
    public void checkAggregationEtag(String etagItemListHashKey, String etagKey, String etag,
                                     Handler<AsyncResult<Boolean>> resultHandler) {
        RedisUtils.performJedisWithRetry(REDIS_CLIENT, ir -> ir.hget(etagItemListHashKey, etagKey, getRes -> {
            if (getRes.succeeded() && getRes.result() != null && getRes.result().equals(etag)) {
                resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
            } else {
                resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
            }
        }));

    }
}
