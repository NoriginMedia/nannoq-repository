package com.nannoq.tools.repository.repository.etag;

import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.utils.ItemList;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.redis.RedisClient;

import java.util.Map;
import java.util.function.Consumer;

/**
 * User: anders
 * Date: 04.12.17 10:31
 */
public interface ETagManager<E extends Model & ETagable> {
    void removeProjectionsEtags(String hash, Handler<AsyncResult<Boolean>> resultHandler);
    void destroyEtags(String hash, Handler<AsyncResult<Boolean>> resultHandler);
    void replaceAggregationEtag(String etagItemListHashKey, String etagKey, String newEtag,
                                Handler<AsyncResult<Boolean>> resultHandler);

    void setSingleRecordEtag(Map<String, String> etagMap, Handler<AsyncResult<Consumer<RedisClient>>> resultHandler);
    void setProjectionEtags(String[] projections, String hash, E item);
    void setItemListEtags(String hash, String etagKey, ItemList<E> itemList, Future<Boolean> itemListEtagFuture);
}
