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
 * @author Anders Mikkelsen
 * @version 17.11.2017
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
