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

package com.nannoq.tools.repository.repository.etag;

import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.utils.ItemList;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.Map;

/**
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public interface ETagManager<E extends Model & ETagable> {
    void removeProjectionsEtags(int hash, Handler<AsyncResult<Boolean>> resultHandler);
    void destroyEtags(int hash, Handler<AsyncResult<Boolean>> resultHandler);
    void replaceAggregationEtag(String etagItemListHashKey, String etagKey, String newEtag,
                                Handler<AsyncResult<Boolean>> resultHandler);

    void setSingleRecordEtag(Map<String, String> etagMap, Handler<AsyncResult<Boolean>> resultHandler);
    void setProjectionEtags(String[] projections, int hash, E item);
    void setItemListEtags(String etagItemListHashKey, String etagKey, ItemList<E> itemList, Future<Boolean> itemListEtagFuture);

    void checkItemEtag(String etagKeyBase, String key, String requestEtag, Handler<AsyncResult<Boolean>> resultHandler);
    void checkItemListEtag(String etagItemListHashKey, String etagKey, String etag, Handler<AsyncResult<Boolean>> resultHandler);
    void checkAggregationEtag(String etagItemListHashKey, String etagKey, String etag, Handler<AsyncResult<Boolean>> resultHandler);
}
