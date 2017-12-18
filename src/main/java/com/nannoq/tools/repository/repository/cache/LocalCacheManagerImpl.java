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
 */

package com.nannoq.tools.repository.repository.cache;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.utils.ItemList;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.serviceproxy.ServiceException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

/**
 * The cachemanger contains the logic for setting, removing, and replace caches.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class LocalCacheManagerImpl<E extends Model & Cacheable> implements CacheManager<E> {
    private static final Logger logger = LoggerFactory.getLogger(ClusterCacheManagerImpl.class.getSimpleName());

    private final Vertx vertx;
    private final Class<E> TYPE;

    private static boolean cachesCreated = false;

    private final String ITEM_LIST_KEY_MAP;
    private final String AGGREGATION_KEY_MAP;

    private final boolean hasTypeJsonField;

    public LocalCacheManagerImpl(Class<E> type, Vertx vertx) {
        this.TYPE = type;
        this.vertx = vertx;
        this.ITEM_LIST_KEY_MAP = TYPE.getSimpleName() + "/ITEMLIST";
        this.AGGREGATION_KEY_MAP = TYPE.getSimpleName() + "/AGGREGATION";

        hasTypeJsonField = Arrays.stream(type.getDeclaredAnnotations()).anyMatch(a -> a instanceof JsonTypeInfo);
    }

    @Override
    public void initializeCache(Handler<AsyncResult<Boolean>> resultHandler) {
        if (cachesCreated) return;

        cachesCreated = true;

        resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
    }

    @Override
    public void checkObjectCache(String cacheId, Handler<AsyncResult<E>> resultHandler) {
        if (isObjectCacheAvailable()) {
            final String content = getObjectCache().get(cacheId);

            if (content == null) {
                resultHandler.handle(ServiceException.fail(404, "Cache result is null!"));
            } else {
                resultHandler.handle(Future.succeededFuture(Json.decodeValue(content, TYPE)));
            }
        } else {
            logger.error("ObjectCache is null, recreating...");

            resultHandler.handle(ServiceException.fail(404, "Unable to retrieve from cache, cache was null..."));
        }
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void checkItemListCache(String cacheId, String[] projections,
                                   Handler<AsyncResult<ItemList<E>>> resultHandler) {
        if (logger.isDebugEnabled()) {
            logger.debug("Checking Item List Cache");
        }

        if (isItemListCacheAvailable()) {
            final String content = getItemListCache().get(cacheId);


            if (content == null) {
                resultHandler.handle(ServiceException.fail(404, "Cache result is null!"));
            } else {
                try {
                    JsonObject jsonObject = new JsonObject(content);
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

                    final ItemList<E> eItemList = new ItemList<>();
                    eItemList.setItems(items);
                    eItemList.setCount(items.size());
                    eItemList.setEtag(jsonObject.getString("etag"));
                    eItemList.setPageToken(pageToken);

                    resultHandler.handle(Future.succeededFuture(eItemList));
                } catch (DecodeException e) {
                    logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));

                    resultHandler.handle(ServiceException.fail(404, "Cache result is null...",
                            new JsonObject(Json.encode(e))));
                }
            }
        } else {
            logger.error("ItemList Cache is null, recreating...");

            resultHandler.handle(ServiceException.fail(404, "Unable to perform cache fetch, cache was null..."));
        }
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void checkAggregationCache(String cacheKey, Handler<AsyncResult<String>> resultHandler) {
        if (isAggregationCacheAvailable()) {
            final String content = getAggregationCache().get(cacheKey);

            if (content == null) {
                resultHandler.handle(ServiceException.fail(404, "Cache result is null..."));
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Returning cached content...");
                }

                resultHandler.handle(Future.succeededFuture(content));
            }
        } else {
            resultHandler.handle(ServiceException.fail(404, "Cache is null..."));
        }
    }

    @Override
    public void replaceObjectCache(String cacheId, E item, Future<E> future, String[] projections) {
        if (isObjectCacheAvailable()) {
            String fullCacheContent = Json.encode(item);
            String jsonRepresentationCache = item.toJsonFormat(projections).encode();

            getObjectCache().put("FULL_CACHE_" + cacheId, fullCacheContent);
            getObjectCache().put(cacheId, jsonRepresentationCache);

            future.complete(item);
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
            records.forEach(record -> {
                String shortCacheId = shortCacheIdSupplier.apply(record);
                String cacheId = cacheIdSupplier.apply(record);

                getObjectCache().put(cacheId, record.toJsonString());
                getObjectCache().put(shortCacheId, record.toJsonString());

                String secondaryCache = "FULL_CACHE_" + cacheId;
                getObjectCache().put(secondaryCache, Json.encode(record));
                getObjectCache().put("FULL_CACHE_" + shortCacheId, Json.encode(record));
            });

            purgeSecondaryCaches(writeFuture.completer());
        } else {
            logger.error("ObjectCache is null, recreating...");

            purgeSecondaryCaches(writeFuture.completer());
        }
    }

    @Override
    public void replaceItemListCache(String content, Supplier<String> cacheIdSupplier,
                                     Handler<AsyncResult<Boolean>> resultHandler) {
        if (isItemListCacheAvailable()) {
            String cacheId = cacheIdSupplier.get();

            getItemListCache().put(cacheId, content);
            replaceMapValues(ITEM_LIST_KEY_MAP, cacheId);

            resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
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

            getAggregationCache().put(cacheKey, content);
            replaceMapValues(AGGREGATION_KEY_MAP, cacheKey);

            resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
        } else {
            logger.error("AggregationCache is null, recreating...");

            resultHandler.handle(ServiceException.fail(500, "Aggregation cache does not exist!"));
        }
    }

    private void replaceMapValues(String AGGREGATION_KEY_MAP, String cacheKey) {
        final LocalMap<String, String> map = vertx.sharedData().getLocalMap(AGGREGATION_KEY_MAP);
        String idSet = map.get(TYPE.getSimpleName());

        if (idSet == null) {
            idSet = new JsonArray()
                    .add(cacheKey)
                    .encode();

            map.put(TYPE.getSimpleName(), idSet);
        } else {
            map.replace(TYPE.getSimpleName(), new JsonArray(idSet).add(cacheKey).encode());
        }
    }

    @Override
    public void purgeCache(Future<Boolean> future, List<E> records, Function<E, String> cacheIdSupplier) {
        if (isObjectCacheAvailable()) {
            records.forEach(record -> {
                String cacheId = cacheIdSupplier.apply(record);
                String secondaryCache = "FULL_CACHE_" + cacheId;

                getObjectCache().remove(cacheId);
                getObjectCache().remove(secondaryCache);
            });

            purgeSecondaryCaches(future.completer());
        } else {
            logger.error("ObjectCache is null, recreating...");

            purgeSecondaryCaches(future.completer());
        }
    }

    private void purgeSecondaryCaches(Handler<AsyncResult<Boolean>> resultHandler) {
        if (isItemListCacheAvailable()) {
            purgeMap(ITEM_LIST_KEY_MAP, getItemListCache());
        } else {
            logger.error("ItemListCache is null, recreating...");
        }

        if (isAggregationCacheAvailable()) {
            purgeMap(AGGREGATION_KEY_MAP, getAggregationCache());
        } else {
            logger.error("AggregateCache is null, recreating...");
        }

        resultHandler.handle(Future.succeededFuture());
    }

    private void purgeMap(String MAP_KEY, final Map<String, String> cache) {
        try {
            final LocalMap<String, String> localMap = vertx.sharedData().getLocalMap(MAP_KEY);

            try {
                String cachePartitionKey = TYPE.newInstance().getCachePartitionKey();

                final String strings = localMap.get(cachePartitionKey);

                if (strings != null) {
                    new JsonArray(strings).stream()
                            .map(Object::toString)
                            .forEach(cache::remove);
                } else {
                    localMap.put(cachePartitionKey, new JsonArray().encode());
                }
            } catch (InstantiationException | IllegalAccessException e) {
                logger.error("Unable to build partitionKey", e);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Cache cleared: " + cache.size());
            }
        } catch (Exception e) {
            logger.error(e);
            logger.error("Unable to purge cache, nulling...");

            cache.clear();
        }
    }

    private LocalMap<String, String> getObjectCache() {
        return vertx.sharedData().getLocalMap("objectCache");
    }

    private LocalMap<String, String> getItemListCache() {
        return vertx.sharedData().getLocalMap("itemListCache");
    }

    private LocalMap<String, String> getAggregationCache() {
        return vertx.sharedData().getLocalMap("aggregationCache");
    }

    @Override
    public Boolean isObjectCacheAvailable() {
        boolean available = getObjectCache() != null;

        if (!available) {
            getObjectCache();
        }

        return available;
    }

    @Override
    public Boolean isItemListCacheAvailable() {
        boolean available = getItemListCache() != null;

        if (!available) {
            getItemListCache();
        }

        return available;
    }

    @Override
    public Boolean isAggregationCacheAvailable() {
        boolean available = getAggregationCache() != null;

        if (!available) {
            getAggregationCache();
        }

        return available;
    }
}
