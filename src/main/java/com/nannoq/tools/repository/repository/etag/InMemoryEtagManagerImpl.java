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
import io.vertx.core.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.LocalMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The cachemanger contains the logic for setting, removing, and replace etags.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class InMemoryEtagManagerImpl<E extends ETagable & Model> implements ETagManager<E> {
    private Logger logger = LoggerFactory.getLogger(InMemoryEtagManagerImpl.class.getSimpleName());

    private final Vertx vertx;
    private final Class<E> TYPE;

    private static final String OBJECT_ETAG_MAP = "OBJECT_ETAG_MAP";

    public InMemoryEtagManagerImpl(Class<E> TYPE) {
        this(Vertx.currentContext().owner(), TYPE);
    }

    public InMemoryEtagManagerImpl(Vertx vertx, Class<E> TYPE) {
        this.vertx = vertx;
        this.TYPE = TYPE;
    }

    private LocalMap<String, String> getLocalObjectMap() {
        return vertx.sharedData().getLocalMap(OBJECT_ETAG_MAP);
    }

    private LocalMap<String, String> getLocalObjectMap(String etagKeyBase) {
        return vertx.sharedData().getLocalMap(etagKeyBase);
    }

    private LocalMap<String, String> getLocalItemListMap(String itemListHashKey) {
        return vertx.sharedData().getLocalMap(itemListHashKey);
    }

    private void getClusteredObjectMap(Handler<AsyncResult<AsyncMap<String, String>>> resultHandler) {
        if (!vertx.isClustered()) throw new IllegalStateException("Vertx is not clustered!");
        vertx.sharedData().getClusterWideMap(OBJECT_ETAG_MAP, resultHandler);
    }

    private void getClusteredObjectMap(String etagKeyBase, Handler<AsyncResult<AsyncMap<String, String>>> resultHandler) {
        if (!vertx.isClustered()) throw new IllegalStateException("Vertx is not clustered!");
        vertx.sharedData().getClusterWideMap(etagKeyBase, resultHandler);
    }

    private void getClusteredItemListMap(String itemListHashKey,
                                         Handler<AsyncResult<AsyncMap<String, String>>> resultHandler) {
        if (!vertx.isClustered()) throw new IllegalStateException("Vertx is not clustered!");
        vertx.sharedData().getClusterWideMap(itemListHashKey, resultHandler);
    }

    public void removeProjectionsEtags(int hash, Handler<AsyncResult<Boolean>> resultHandler) {
        String etagKeyBase = TYPE.getSimpleName() + "_" + hash + "/projections";

        if (vertx.isClustered()) {
            getClusteredObjectMap(etagKeyBase, mapRes -> clearClusteredMap(resultHandler, mapRes));
        } else {
            getLocalItemListMap(etagKeyBase).clear();

            resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
        }
    }

    public void destroyEtags(int hash, Handler<AsyncResult<Boolean>> resultHandler) {
        String etagItemListHashKey = TYPE.getSimpleName() + "_" + hash + "_" + "itemListEtags";

        if (vertx.isClustered()) {
            getClusteredItemListMap(etagItemListHashKey, mapRes -> clearClusteredMap(resultHandler, mapRes));
        } else {
            getLocalItemListMap(etagItemListHashKey).clear();

            resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
        }
    }

    private void clearClusteredMap(Handler<AsyncResult<Boolean>> resultHandler,
                                   AsyncResult<AsyncMap<String, String>> mapRes) {
        if (mapRes.failed()) {
            resultHandler.handle(Future.failedFuture(mapRes.cause()));
        } else {
            mapRes.result().clear(clearRes -> {
                if (clearRes.failed()) {
                    resultHandler.handle(Future.failedFuture(clearRes.cause()));
                } else {
                    resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                }
            });
        }
    }

    public void replaceAggregationEtag(String etagItemListHashKey, String etagKey, String newEtag,
                                       Handler<AsyncResult<Boolean>> resultHandler) {
        if (vertx.isClustered()) {
            getClusteredItemListMap(etagItemListHashKey, res -> {
                if (res.failed()) {
                    resultHandler.handle(Future.failedFuture(res.cause()));
                } else {
                    res.result().put(etagKey, newEtag, setRes -> {
                        if (setRes.failed()) {
                            logger.error("Unable to set etag!", setRes.cause());

                            resultHandler.handle(Future.failedFuture(setRes.cause()));
                        } else {
                            resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                        }
                    });
                }
            });
        } else {
            getLocalItemListMap(etagItemListHashKey).put(etagKey, newEtag);

            resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
        }
    }

    @Override
    public void setSingleRecordEtag(Map<String, String> etagMap,
                                    Handler<AsyncResult<Boolean>> resultHandler) {
        if (vertx.isClustered()) {
            getClusteredObjectMap(mapRes -> {
                if (mapRes.failed()) {
                    logger.error("Failed etag setting for objects!", mapRes.cause());

                    resultHandler.handle(Future.failedFuture(mapRes.cause()));
                } else {
                    final AsyncMap<String, String> map = mapRes.result();
                    List<Future> setFutures = new ArrayList<>();

                    etagMap.forEach((k, v) -> {
                        Future<Void> setFuture = Future.future();

                        map.put(k, v, setFuture.completer());

                        setFutures.add(setFuture);
                    });

                    CompositeFuture.all(setFutures).setHandler(res -> {
                        if (res.failed()) {
                            resultHandler.handle(Future.failedFuture(res.cause()));
                        } else {
                            resultHandler.handle(Future.succeededFuture());
                        }
                    });
                }
            });
        } else {
            final LocalMap<String, String> localObjectMap = getLocalObjectMap();
            etagMap.forEach(localObjectMap::put);

            resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
        }
    }

    @Override
    public void setProjectionEtags(String[] projections, int hash, E item) {
        if (projections != null && projections.length > 0 && item != null) {
            String etagKeyBase = TYPE.getSimpleName() + "_" + hash + "/projections";
            String key = TYPE.getSimpleName() + "_" + hash + "/projections" + Arrays.hashCode(projections);
            String etag = item.getEtag();

            if (vertx.isClustered()) {
                getClusteredObjectMap(etagKeyBase, mapRes -> {
                    if (mapRes.succeeded()) {
                        mapRes.result().put(key, etag, setRes -> {});
                    }
                });
            } else {
                getLocalObjectMap(etagKeyBase).put(key, etag);
            }
        }
    }

    @Override
    public void setItemListEtags(String etagItemListHashKey, String etagKey, ItemList<E> itemList,
                                 Future<Boolean> itemListEtagFuture)  {
        setItemListEtags(etagItemListHashKey, etagKey, itemList.getEtag(), itemListEtagFuture);
    }

    private void setItemListEtags(String etagItemListHashKey, String etagKey, String etag,
                                  Future<Boolean> itemListEtagFuture) {
        if (vertx.isClustered()) {
            getClusteredItemListMap(etagItemListHashKey, res -> {
                if (res.failed()) {
                    itemListEtagFuture.fail(res.cause());
                } else {
                    res.result().put(etagKey, etag, setRes -> {
                        if (setRes.failed()) {
                            logger.error("Unable to set etag!", setRes.cause());

                            itemListEtagFuture.fail(setRes.cause());
                        } else {
                            itemListEtagFuture.complete(Boolean.TRUE);
                        }
                    });
                }
            });
        } else {
            getLocalItemListMap(etagItemListHashKey).put(etagKey, etag);

            itemListEtagFuture.complete(Boolean.TRUE);
        }
    }

    @Override
    public void checkItemEtag(String etagKeyBase, String key, String etag,
                              Handler<AsyncResult<Boolean>> resultHandler) {
        if (vertx.isClustered()) {
            getClusteredObjectMap(etagKeyBase, res -> checkEtagFromMap(key, etag, resultHandler, res));
        } else {
            final String s = getLocalObjectMap().get(key);

            if (s != null) {
                resultHandler.handle(Future.succeededFuture(s.equals(etag)));
            } else {
                resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
            }
        }
    }

    @Override
    public void checkItemListEtag(String etagItemListHashKey, String etagKey, String etag,
                                  Handler<AsyncResult<Boolean>> resultHandler) {
        if (vertx.isClustered()) {
            getClusteredItemListMap(etagItemListHashKey, res -> checkEtagFromMap(etagKey, etag, resultHandler, res));
        } else {
            final String s = getLocalItemListMap(etagItemListHashKey).get(etagKey);

            if (s != null) {
                resultHandler.handle(Future.succeededFuture(s.equals(etag)));
            } else {
                resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
            }
        }
    }

    @Override
    public void checkAggregationEtag(String etagItemListHashKey, String etagKey, String etag,
                                     Handler<AsyncResult<Boolean>> resultHandler) {
        checkItemListEtag(etagItemListHashKey, etagKey, etag, resultHandler);
    }

    private void checkEtagFromMap(String key, String etag, Handler<AsyncResult<Boolean>> resultHandler,
                                  AsyncResult<AsyncMap<String, String>> res) {
        if (res.failed()) {
            resultHandler.handle(Future.failedFuture(res.cause()));
        } else {
            res.result().get(key, getRes -> {
                if (getRes.failed()) {
                    logger.error("Unable to set etag!", getRes.cause());

                    resultHandler.handle(Future.failedFuture(getRes.cause()));
                } else {
                    if (getRes.result() != null && getRes.result().equals(etag)) {
                        resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                    } else {
                        resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                    }
                }
            });
        }
    }
}
