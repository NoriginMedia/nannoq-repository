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

package com.nannoq.tools.repository.repository.cache;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.ICache;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.utils.ItemList;
import io.vertx.core.*;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.serviceproxy.ServiceException;

import javax.cache.CacheException;
import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.spi.CachingProvider;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;
import static javax.cache.expiry.Duration.FIVE_MINUTES;

/**
 * The cachemanger contains the logic for setting, removing, and replace caches.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class ClusterCacheManagerImpl<E extends Cacheable & Model> implements CacheManager<E> {
    private static final Logger logger = LoggerFactory.getLogger(ClusterCacheManagerImpl.class.getSimpleName());

    private final Vertx vertx;
    private final Class<E> TYPE;

    private static boolean cachesCreated = false;
    private static ICache<String, String> objectCache;
    private static ICache<String, String> itemListCache;
    private static ICache<String, String> aggregationCache;

    private final String ITEM_LIST_KEY_MAP;
    private final String AGGREGATION_KEY_MAP;

    private final long CACHE_TIMEOUT_VALUE = 1000L;
    private ExpiryPolicy expiryPolicy = AccessedExpiryPolicy.factoryOf(FIVE_MINUTES).create();

    private final boolean hasTypeJsonField;

    public ClusterCacheManagerImpl(Class<E> type, Vertx vertx) {
        this.TYPE = type;
        this.vertx = vertx;
        this.ITEM_LIST_KEY_MAP = TYPE.getSimpleName() + "/ITEMLIST";
        this.AGGREGATION_KEY_MAP = TYPE.getSimpleName() + "/AGGREGATION";

        hasTypeJsonField = Arrays.stream(type.getDeclaredAnnotations()).anyMatch(a -> a instanceof JsonTypeInfo);
    }

    @Override
    public void initializeCache(Handler<AsyncResult<Boolean>> resultHandler) {
        if (cachesCreated) return;

        vertx.<Boolean>executeBlocking(future -> {
            try {
                objectCache = createCache("object");
                itemListCache = createCache("itemList");
                aggregationCache = createCache("aggregation");

                future.complete(true);
            } catch (CacheException e) {
                logger.error("Cache creation interrupted: " + e.getMessage());

                future.fail(e);
            }
        }, false, res -> {
            if (res.failed()) {
                resultHandler.handle(Future.failedFuture(res.cause()));
            } else {
                cachesCreated = true;

                resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
            }
        });
    }

    private ICache<String, String> createCache(String cacheName) {
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        Optional<HazelcastInstance> hzOpt = instances.stream().findFirst();

        if (hzOpt.isPresent()) {
            HazelcastInstance hz = hzOpt.get();

            try {
                @SuppressWarnings("UnnecessaryLocalVariable")
                ICache<String, String> cache = hz.getCacheManager().getCache(cacheName);

                logger.info("Initialized cache: " + cache.getName() + " ok!");

                return cache;
            } catch (CacheNotExistsException cnee) {
                CachingProvider cachingProvider = Caching.getCachingProvider();
                CompleteConfiguration<String, String> config =
                        new MutableConfiguration<String, String>()
                                .setTypes(String.class, String.class)
                                .setManagementEnabled(false)
                                .setStatisticsEnabled(false)
                                .setReadThrough(false)
                                .setWriteThrough(false);

                //noinspection unchecked
                return cachingProvider.getCacheManager().createCache(cacheName, config).unwrap(ICache.class);
            } catch (IllegalStateException ilse) {
                logger.error("JCache not available!");

                return null;
            }
        } else {
            logger.error("Cannot find hazelcast instance!");

            return null;
        }
    }

    @Override
    public void checkObjectCache(String cacheId, Handler<AsyncResult<E>> resultHandler) {
        if (isObjectCacheAvailable()) {
            AtomicBoolean completeOrTimeout = new AtomicBoolean();
            completeOrTimeout.set(false);

            vertx.setTimer(CACHE_TIMEOUT_VALUE, aLong -> {
                if (!completeOrTimeout.getAndSet(true)) {
                    resultHandler.handle(ServiceException.fail(502, "Cache timeout!"));
                }
            });

            objectCache.getAsync(cacheId).andThen(new ExecutionCallback<String>() {
                @Override
                public void onResponse(String s) {
                    if (!completeOrTimeout.getAndSet(true)) {
                        try {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Cached Content is: " + s);
                            }

                            if (s == null) {
                                resultHandler.handle(ServiceException.fail(404, "Cache result is null!"));
                            } else {
                                resultHandler.handle(Future.succeededFuture(Json.decodeValue(s, TYPE)));
                            }
                        } catch (DecodeException e) {
                            logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));

                            resultHandler.handle(ServiceException.fail(404, "Cache result is null...",
                                    new JsonObject(Json.encode(e))));
                        }
                    } else {
                        resultHandler.handle(ServiceException.fail(502, "Cache timeout!"));
                    }
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.error(throwable + " : " + throwable.getMessage() + " : " +
                            Arrays.toString(throwable.getStackTrace()));

                    if (!completeOrTimeout.getAndSet(true)) {
                        resultHandler.handle(ServiceException.fail(500, "Unable to retrieve from cache...",
                                new JsonObject(Json.encode(throwable))));
                    }
                }
            });
        } else {
            logger.error("ObjectCache is null, recreating...");

            resultHandler.handle(ServiceException.fail(404, "Unable to retrieve from cache, cache was null..."));
        }
    }

    @Override
    public void checkItemListCache(String cacheId, String[] projections,
                                   Handler<AsyncResult<ItemList<E>>> resultHandler) {
        if (logger.isDebugEnabled()) {
            logger.debug("Checking Item List Cache");
        }

        if (isItemListCacheAvailable()) {
            AtomicBoolean completeOrTimeout = new AtomicBoolean();
            completeOrTimeout.set(false);

            vertx.setTimer(CACHE_TIMEOUT_VALUE, aLong -> {
                if (!completeOrTimeout.getAndSet(true)) {
                    resultHandler.handle(ServiceException.fail(502, "Cache timeout!"));
                }
            });

            itemListCache.getAsync(cacheId).andThen(new ExecutionCallback<String>() {
                @SuppressWarnings("unchecked")
                @Override
                public void onResponse(String s) {
                    if (!completeOrTimeout.getAndSet(true)) {
                        if (s == null) {
                            resultHandler.handle(ServiceException.fail(404, "Cache result is null!"));
                        } else {
                            try {
                                JsonObject jsonObject = new JsonObject(s);
                                JsonArray jsonArray = jsonObject.getJsonArray("items");
                                String pageToken = jsonObject.getString("pageToken");
                                List<E> items = jsonArray.stream()
                                        .map(json -> {
                                            JsonObject obj = new JsonObject(json.toString());

                                            if (hasTypeJsonField) {
                                                obj.put("@type", TYPE.getSimpleName());
                                            }

                                            return Json.decodeValue(obj.encode(), TYPE);
                                        })
                                        .collect(toList());

                                final ItemList<E> eItemList =
                                        new ItemList<>(pageToken, items.size(), items, projections);
                                eItemList.setEtag(jsonObject.getString("etag"));

                                resultHandler.handle(Future.succeededFuture(eItemList));
                            } catch (DecodeException e) {
                                logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));

                                resultHandler.handle(ServiceException.fail(404, "Cache result is null...",
                                        new JsonObject(Json.encode(e))));
                            }
                        }
                    }
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.error(throwable + " : " + throwable.getMessage() + " : " +
                            Arrays.toString(throwable.getStackTrace()));

                    if (!completeOrTimeout.getAndSet(true)) {
                        resultHandler.handle(ServiceException.fail(500, "Cache fetch failed...",
                                new JsonObject(Json.encode(throwable))));
                    }
                }
            });
        } else {
            logger.error("ItemList Cache is null, recreating...");

            resultHandler.handle(ServiceException.fail(404, "Unable to perform cache fetch, cache was null..."));
        }
    }

    @Override
    public void checkAggregationCache(String cacheKey, Handler<AsyncResult<String>> resultHandler) {
        if (isAggregationCacheAvailable()) {
            AtomicBoolean completeOrTimeout = new AtomicBoolean();
            completeOrTimeout.set(false);

            vertx.setTimer(10000L, aLong -> {
                if (!completeOrTimeout.getAndSet(true)) {
                    resultHandler.handle(
                            ServiceException.fail(502, "Cache timeout!"));
                }
            });

            aggregationCache.getAsync(cacheKey, expiryPolicy).andThen(new ExecutionCallback<String>() {

                public void onResponse(String s) {
                    if (!completeOrTimeout.getAndSet(true)) {
                        if (s == null) {
                            resultHandler.handle(ServiceException.fail(404, "Cache result is null..."));
                        } else {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Returning cached content...");
                            }

                            resultHandler.handle(Future.succeededFuture(s));
                        }
                    }
                }


                public void onFailure(Throwable throwable) {
                    logger.error(throwable + " : " + throwable.getMessage() + " : " +
                            Arrays.toString(throwable.getStackTrace()));

                    if (!completeOrTimeout.getAndSet(true)) {
                        resultHandler.handle(ServiceException.fail(500,
                                "Unable to retrieve from cache...", new JsonObject(Json.encode(throwable))));
                    }
                }
            });
        } else {
            resultHandler.handle(ServiceException.fail(404, "Cache is null..."));
        }
    }

    @Override
    public void replaceObjectCache(String cacheId, E item, Future<E> future, String[] projections) {
        if (isObjectCacheAvailable()) {
            String fullCacheContent = Json.encode(item);
            String jsonRepresentationCache = item.toJsonFormat(projections).encode();
            Future<Boolean> fullCacheFuture = Future.future();
            Future<Boolean> jsonFuture = Future.future();

            vertx.setTimer(CACHE_TIMEOUT_VALUE, aLong -> vertx.executeBlocking(fut -> {
                if (!fullCacheFuture.isComplete()) {
                    objectCache.removeAsync("FULL_CACHE_" + cacheId);
                    fullCacheFuture.tryComplete();

                    logger.error("Cache timeout!");
                }

                if (!jsonFuture.isComplete()) {
                    objectCache.removeAsync(cacheId);
                    jsonFuture.tryComplete();

                    logger.error("Cache timeout!");
                }

                fut.complete();
            }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded())));

            objectCache.putAsync("FULL_CACHE_" + cacheId, fullCacheContent, expiryPolicy).andThen(new ExecutionCallback<Void>() {
                @Override
                public void onResponse(Void b) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Set new cache on: " + cacheId + " is " + b);
                    }

                    fullCacheFuture.tryComplete(Boolean.TRUE);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.error(throwable + " : " + throwable.getMessage() + " : " +
                            Arrays.toString(throwable.getStackTrace()));

                    fullCacheFuture.tryFail(throwable);
                }
            });

            objectCache.putAsync(cacheId, jsonRepresentationCache, expiryPolicy).andThen(new ExecutionCallback<Void>() {
                @Override
                public void onResponse(Void b) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Set new cache on: " + cacheId + " is " + b);
                    }

                    jsonFuture.tryComplete(Boolean.TRUE);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.error(throwable + " : " + throwable.getMessage() + " : " +
                            Arrays.toString(throwable.getStackTrace()));

                    jsonFuture.tryFail(throwable);
                }
            });

            CompositeFuture.all(fullCacheFuture, jsonFuture).setHandler(cacheRes -> {
                if (cacheRes.failed()) {
                    future.fail(cacheRes.cause());
                } else {
                    future.complete(item);
                }
            });
        } else {
            logger.error("ObjectCache is null, recreating...");

            future.complete(item);
        }
    }

    @Override
    public void replaceCache(Future<Boolean> writeFuture, List<E> records,
                             Function<E, String> shortCacheIdSupplier,
                             Function<E, String> cacheIdSupplier) {
        if (isObjectCacheAvailable()) {
            List<Future> replaceFutures = new ArrayList<>();

            records.forEach(record -> {
                Future<Boolean> replaceFuture = Future.future();
                String shortCacheId = shortCacheIdSupplier.apply(record);
                String cacheId = cacheIdSupplier.apply(record);

                Future<Boolean> rFirst = Future.future();
                replaceTimeoutHandler(cacheId, rFirst);
                replace(rFirst, cacheId, record.toJsonString());

                Future<Boolean> rFirstRoot = Future.future();
                replaceTimeoutHandler(shortCacheId, rFirstRoot);
                replace(rFirstRoot, shortCacheId, record.toJsonString());

                String secondaryCache = "FULL_CACHE_" + cacheId;
                Future<Boolean> rSecond = Future.future();
                replaceTimeoutHandler(secondaryCache, rSecond);
                replace(rSecond, secondaryCache, Json.encode(record));

                Future<Boolean> rSecondRoot = Future.future();
                replaceTimeoutHandler(cacheId, rSecondRoot);
                replace(rSecondRoot, "FULL_CACHE_" + shortCacheId, Json.encode(record));

                CompositeFuture.all(rFirst, rSecond, rFirstRoot, rSecondRoot).setHandler(purgeRes -> {
                    if (purgeRes.succeeded()) {
                        replaceFuture.complete(Boolean.TRUE);
                    } else {
                        replaceFuture.fail(purgeRes.cause());
                    }
                });

                replaceFutures.add(replaceFuture);
            });

            CompositeFuture.all(replaceFutures).setHandler(res -> purgeSecondaryCaches(writeFuture.completer()));
        } else {
            logger.error("ObjectCache is null, recreating...");

            purgeSecondaryCaches(writeFuture.completer());
        }
    }

    private void replace(Future<Boolean> replaceFuture, String cacheId, String recordAsJson) {
        if (isObjectCacheAvailable()) {
            objectCache.putAsync(cacheId, recordAsJson, expiryPolicy).andThen(new ExecutionCallback<Void>() {
                @Override
                public void onResponse(Void b) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cache Replaced for: " + cacheId + " is " + b);
                    }

                    replaceFuture.tryComplete(Boolean.TRUE);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.error(throwable + " : " + throwable.getMessage() + " : " +
                            Arrays.toString(throwable.getStackTrace()));

                    replaceFuture.tryComplete(Boolean.FALSE);
                }
            });
        } else {
            replaceFuture.tryComplete(Boolean.FALSE);
        }
    }

    private void replaceTimeoutHandler(String cacheId, Future<Boolean> replaceFirst) {
        vertx.setTimer(CACHE_TIMEOUT_VALUE, aLong -> vertx.executeBlocking(future -> {
            if (!replaceFirst.isComplete()) {
                objectCache.removeAsync(cacheId);

                replaceFirst.tryComplete(Boolean.TRUE);

                logger.error("Cache timeout!");
            }

            future.complete();
        }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded())));
    }

    @Override
    public void replaceItemListCache(String content, Supplier<String> cacheIdSupplier,
                                     Handler<AsyncResult<Boolean>> resultHandler) {
        if (isItemListCacheAvailable()) {
            String cacheId = cacheIdSupplier.get();
            Future<Boolean> cacheFuture = Future.future();

            vertx.setTimer(CACHE_TIMEOUT_VALUE, aLong -> vertx.executeBlocking(fut -> {
                if (!cacheFuture.isComplete()) {
                    itemListCache.removeAsync(cacheId);

                    cacheFuture.tryFail(new TimeoutException("Cache request timed out, above: " + CACHE_TIMEOUT_VALUE + "!"));

                    logger.error("Cache timeout!");
                }

                fut.complete();
            }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded())));

            itemListCache.putAsync(cacheId, content, expiryPolicy).andThen(new ExecutionCallback<Void>() {
                @Override
                public void onResponse(Void b) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Set new cache on: " + cacheId + " is " + b);
                    }

                    replaceMapValues(cacheFuture, ITEM_LIST_KEY_MAP, cacheId);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.error(throwable + " : " + throwable.getMessage() + " : " +
                            Arrays.toString(throwable.getStackTrace()));

                    cacheFuture.tryComplete();
                }
            });

            cacheFuture.setHandler(res -> {
                if (res.failed()) {
                    resultHandler.handle(ServiceException.fail(504, "Cache timed out!"));
                } else {
                    resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                }
            });
        } else {
            logger.error("ItemListCache is null, recreating...");

            resultHandler.handle(ServiceException.fail(500, "Itemlist cache does not exist!"));
        }
    }

    @Override
    public void replaceAggregationCache(String content, Supplier<String> cacheIdSupplier,
                                        Handler<AsyncResult<Boolean>> resultHandler) {
        if (isAggregationCacheAvailable()) {
            String cacheKey = cacheIdSupplier.get();

            Future<Boolean> cacheIdFuture = Future.future();
            vertx.setTimer(CACHE_TIMEOUT_VALUE, aLong -> vertx.executeBlocking(future -> {
                if (!cacheIdFuture.isComplete()) {
                    aggregationCache.removeAsync(cacheKey);

                    cacheIdFuture.tryComplete();

                    logger.error("Cache timeout!");
                }

                future.complete();
            }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded())));

            aggregationCache.putAsync(cacheKey, content, expiryPolicy).andThen(new ExecutionCallback<Void>() {

                public void onResponse(Void b) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Set cache for " + cacheKey + " is " + b);
                    }

                    replaceMapValues(cacheIdFuture, AGGREGATION_KEY_MAP, cacheKey);
                }


                public void onFailure(Throwable throwable) {
                    logger.error(throwable);

                    cacheIdFuture.tryComplete();
                }
            });

            cacheIdFuture.setHandler(complete -> {
                if (complete.failed()) {
                    resultHandler.handle(ServiceException.fail(500, "Timeout on cache!"));
                } else {
                    resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                }
            });
        } else {
            logger.error("AggregationCache is null, recreating...");

            resultHandler.handle(ServiceException.fail(500, "Aggregation cache does not exist!"));
        }
    }

    private void replaceMapValues(Future<Boolean> cacheIdFuture, String AGGREGATION_KEY_MAP, String cacheKey) {
        vertx.sharedData().<String, Set<String>>getClusterWideMap(AGGREGATION_KEY_MAP, map -> {
            if (map.failed()) {
                logger.error("Cannot set cachemap...", map.cause());

                cacheIdFuture.tryComplete();
            } else {
                map.result().get(TYPE.getSimpleName(), set -> {
                    if (set.failed()) {
                        logger.error("Unable to get TYPE id set!", set.cause());

                        cacheIdFuture.tryComplete();
                    } else {
                        Set<String> idSet = set.result();

                        if (idSet == null) {
                            idSet = new HashSet<>();

                            idSet.add(cacheKey);

                            map.result().put(TYPE.getSimpleName(), idSet, setRes -> {
                                if (setRes.failed()) {
                                    logger.error("Unable to set cacheIdSet!", setRes.cause());
                                }

                                cacheIdFuture.tryComplete();
                            });
                        } else {
                            idSet.add(cacheKey);

                            map.result().replace(TYPE.getSimpleName(), idSet, setRes -> {
                                if (setRes.failed()) {
                                    logger.error("Unable to set cacheIdSet!", setRes.cause());
                                }

                                cacheIdFuture.tryComplete();
                            });
                        }
                    }
                });
            }
        });
    }

    @Override
    public void purgeCache(Future<Boolean> future, List<E> records, Function<E, String> cacheIdSupplier) {
        if (isObjectCacheAvailable()) {
            List<Future> purgeFutures = new ArrayList<>();

            records.forEach(record -> {
                Future<Boolean> purgeFuture = Future.future();
                String cacheId = cacheIdSupplier.apply(record);
                Future<Boolean> purgeFirst = Future.future();

                objectCache.removeAsync(cacheId).andThen(new ExecutionCallback<Boolean>() {
                    @Override
                    public void onResponse(Boolean b) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Cache Removal on " + cacheId + " success: " + b);
                        }

                        purgeFirst.tryComplete(Boolean.TRUE);
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        logger.error(throwable + " : " + throwable.getMessage() + " : " +
                                Arrays.toString(throwable.getStackTrace()));

                        purgeFirst.tryComplete(Boolean.TRUE);
                    }
                });

                String secondaryCache = "FULL_CACHE_" + cacheId;
                Future<Boolean> purgeSecond = Future.future();
                vertx.setTimer(CACHE_TIMEOUT_VALUE, aLong -> vertx.executeBlocking(fut -> {
                    if (!purgeFirst.isComplete()) {
                        objectCache.removeAsync(cacheId);

                        purgeFirst.tryComplete();

                        logger.error("Cache timeout!");
                    }

                    if (!purgeSecond.isComplete()) {
                        objectCache.removeAsync(secondaryCache);

                        purgeSecond.tryComplete();

                        logger.error("Cache timeout!");
                    }

                    fut.complete();
                }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded())));

                objectCache.removeAsync(secondaryCache).andThen(new ExecutionCallback<Boolean>() {
                    @Override
                    public void onResponse(Boolean b) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Full Cache Removal on " + cacheId + " success: " + b);
                        }

                        purgeSecond.tryComplete(Boolean.TRUE);
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        logger.error(throwable + " : " + throwable.getMessage() + " : " +
                                Arrays.toString(throwable.getStackTrace()));

                        purgeSecond.tryComplete(Boolean.TRUE);
                    }
                });

                CompositeFuture.all(purgeFirst, purgeSecond).setHandler(purgeRes -> {
                    if (purgeRes.succeeded()) {
                        purgeFuture.complete(Boolean.TRUE);
                    } else {
                        purgeFuture.fail(purgeRes.cause());
                    }
                });

                purgeFutures.add(purgeFuture);
            });

            CompositeFuture.all(purgeFutures).setHandler(res -> purgeSecondaryCaches(future.completer()));
        } else {
            logger.error("ObjectCache is null, recreating...");

            purgeSecondaryCaches(future.completer());
        }
    }

    private void purgeSecondaryCaches(Handler<AsyncResult<Boolean>> resultHandler) {
        Future<Boolean> itemListFuture = Future.future();
        Future<Boolean> aggregationFuture = Future.future();

        if (isItemListCacheAvailable()) {
            vertx.setTimer(CACHE_TIMEOUT_VALUE, aLong -> vertx.executeBlocking(future -> {
                if (!itemListFuture.isComplete()) {
                    itemListCache.clear();

                    itemListFuture.tryComplete();

                    logger.error("Cache Timeout!");
                }

                future.complete();
            }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded())));

            purgeMap(ITEM_LIST_KEY_MAP, itemListCache, res -> {
                if (res.failed()) {
                    itemListCache = null;
                }

                itemListFuture.tryComplete();
            });
        } else {
            logger.error("ItemListCache is null, recreating...");

            itemListFuture.tryComplete();
        }

        if (isAggregationCacheAvailable()) {
            vertx.setTimer(CACHE_TIMEOUT_VALUE, aLong -> vertx.executeBlocking(future -> {
                if (!aggregationFuture.isComplete()) {
                    aggregationCache.clear();

                    aggregationFuture.tryComplete();

                    logger.error("Cache timeout!");
                }

                future.complete();
            }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded())));

            purgeMap(AGGREGATION_KEY_MAP, aggregationCache, res -> {
                if (res.failed()) {
                    aggregationCache = null;
                }

                aggregationFuture.tryComplete();
            });
        } else {
            logger.error("AggregateCache is null, recreating...");

            aggregationFuture.tryComplete();
        }

        CompositeFuture.any(itemListFuture, aggregationFuture).setHandler(res ->
                resultHandler.handle(Future.succeededFuture()));
    }

    private void purgeMap(String MAP_KEY, final ICache<String, String> cache,
                          Handler<AsyncResult<Boolean>> resultHandler) {
        vertx.<Boolean>executeBlocking(purgeAllListCaches -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Now purging cache");
            }

            try {
                vertx.sharedData().<String, Set<String>>getClusterWideMap(MAP_KEY, map -> {
                    if (map.failed()) {
                        logger.error("Cannot get cachemap...", map.cause());

                        vertx.executeBlocking(future -> {
                            cache.clear();
                            purgeAllListCaches.tryComplete();

                            future.complete();
                        }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded()));
                    } else {
                        try {
                            String cachePartitionKey = TYPE.newInstance().getCachePartitionKey();

                            map.result().get(cachePartitionKey, getSet ->
                                    purgeMapContents(getSet, cache, purgeAllListCaches, cachePartitionKey, map.result()));
                        } catch (InstantiationException | IllegalAccessException e) {
                            logger.error("Unable to build partitionKey", e);

                            purgeAllListCaches.tryFail(e);
                        }
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("Cache cleared: " + cache.size());
                    }
                });
            } catch (Exception e) {
                logger.error(e);
                logger.error("Unable to purge cache, nulling...");

                purgeAllListCaches.tryFail(e);
            }
        }, res -> resultHandler.handle(res.map(res.result())));
    }

    private void purgeMapContents(AsyncResult<Set<String>> getSet, final ICache<String, String> cache,
                                  Future<Boolean> purgeAllListCaches, String cachePartitionKey,
                                  AsyncMap<String, Set<String>> result) {
        if (getSet.failed()) {
            logger.error("Unable to get idSet!", getSet.cause());

            vertx.executeBlocking(future -> {
                cache.clear();
                purgeAllListCaches.tryComplete();

                future.complete();
            }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded()));
        } else {
            Set<String> idSet = getSet.result();

            if (idSet != null) {
                vertx.executeBlocking(future -> {
                    cache.removeAll(getSet.result());

                    purgeAllListCaches.tryComplete();

                    future.complete();
                }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded()));
            } else {
                vertx.executeBlocking(future -> {
                    cache.clear();

                    result.put(cachePartitionKey, new HashSet<>(), setRes -> {
                        if (setRes.failed()) {
                            logger.error("Unable to clear set...", setRes.cause());
                        }

                        purgeAllListCaches.tryComplete();
                    });

                    future.complete();
                }, false, res -> logger.trace("Result of timeout cache clear is: " + res.succeeded()));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void recreateObjectCache() {
        vertx.executeBlocking(future -> {
            objectCache = createCache("object");

            future.complete(true);
        }, false, result -> {
            if (logger.isDebugEnabled()) { logger.debug("Caches ok: " + result.result()); }
        });
    }

    @SuppressWarnings("unchecked")
    private void recreateItemListCache() {
        vertx.executeBlocking(future -> {
            itemListCache = createCache("itemList");

            future.complete(true);
        }, false, result -> {
            if (logger.isDebugEnabled()) { logger.debug("Caches ok: " + result.result()); }
        });
    }

    @SuppressWarnings("unchecked")
    private void recreateAggregateCache() {
        vertx.executeBlocking(future -> {
            aggregationCache = createCache("aggregation");

            future.complete(true);
        }, false, result -> {
            if (logger.isDebugEnabled()) { logger.debug("Caches ok: " + result.result()); }
        });
    }

    @Override
    public Boolean isObjectCacheAvailable() {
        boolean available = objectCache != null;

        if (!available) {
            recreateObjectCache();
        }

        return available;
    }

    @Override
    public Boolean isItemListCacheAvailable() {
        boolean available = itemListCache != null;

        if (!available) {
            recreateItemListCache();
        }

        return available;
    }

    @Override
    public Boolean isAggregationCacheAvailable() {
        boolean available = aggregationCache != null;

        if (!available) {
            recreateAggregateCache();
        }

        return available;
    }
}
