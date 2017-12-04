package com.nannoq.tools.repository.repository.cache;

import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.utils.ItemList;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * User: anders
 * Date: 04.12.17 10:31
 */
public interface CacheManager<E extends Cacheable & Model> {
    void initializeCache(Handler<AsyncResult<Boolean>> resultHandler);
    void checkObjectCache(String cacheId, Handler<AsyncResult<E>> resultHandler);
    void checkItemListCache(String cacheId, String[] projections, Handler<AsyncResult<ItemList<E>>> resultHandler);
    void checkAggregationCache(String cacheKey, Handler<AsyncResult<String>> resultHandler);

    void replaceCache(Future<Boolean> writeFuture, List<E> records,
                      Function<E, String> shortCacheIdSupplier,
                      Function<E, String> cacheIdSupplier);

    void replaceObjectCache(String cacheId, E item, Future<E> future, String[] projections);
    void replaceItemListCache(String content, Supplier<String> cacheIdSupplier,
                              Handler<AsyncResult<Boolean>> resultHandler);
    void replaceAggregationCache(String content, Supplier<String> cacheIdSupplier,
                                 Handler<AsyncResult<Boolean>> resultHandler);

    void purgeCache(Future<Boolean> future, List<E> records, Function<E, String> cacheIdSupplier);

    Boolean isObjectCacheAvailable();
    Boolean isItemListCacheAvailable();
    Boolean isAggregationCacheAvailable();
}
