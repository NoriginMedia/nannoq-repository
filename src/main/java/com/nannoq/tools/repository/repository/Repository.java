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

package com.nannoq.tools.repository.repository;

import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.models.ModelUtils;
import com.nannoq.tools.repository.repository.etag.ETagManager;
import com.nannoq.tools.repository.repository.results.*;
import com.nannoq.tools.repository.utils.FilterParameter;
import com.nannoq.tools.repository.utils.OrderByParameter;
import com.nannoq.tools.repository.utils.QueryPack;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ServiceException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.AbstractMap.SimpleEntry;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toList;

/**
 * The repository interface is the base of the Repository tools. It defines a contract for all repositories and contains
 * standardized logic.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@SuppressWarnings("unused")
public interface Repository<E extends Model> {
    Logger logger = LoggerFactory.getLogger(Repository.class.getSimpleName());

    String ORDER_BY_KEY = "orderBy";
    String LIMIT_KEY = "limit";
    String PROJECTION_KEY = "projection";
    String MULTIPLE_KEY = "multiple";
    String MULTIPLE_IDS_KEY = "ids";

    enum INCREMENTATION { ADDITION, SUBTRACTION }

    default void create(E record, Handler<AsyncResult<CreateResult<E>>> resultHandler) {
        batchCreate(Collections.singletonList(record), res -> {
            if (res.failed()) {
                resultHandler.handle(Future.failedFuture(res.cause()));
            } else {
                Iterator<CreateResult<E>> iterator = res.result().iterator();

                if (iterator.hasNext()) {
                    resultHandler.handle(Future.succeededFuture(iterator.next()));
                } else {
                    resultHandler.handle(Future.failedFuture(new NullPointerException()));
                }
            }
        });
    }

    default Future<CreateResult<E>> create(E record) {
        Future<CreateResult<E>> createFuture = Future.future();

        create(record, createResult -> {
            if (createResult.failed()) {
                createFuture.fail(createResult.cause());
            } else {
                createFuture.complete(createResult.result());
            }
        });

        return createFuture;
    }

    default void batchCreate(List<E> records, Handler<AsyncResult<List<CreateResult<E>>>> resultHandler) {
        doWrite(true, records.stream()
                .map(r -> new SimpleEntry<E, Function<E, E>>(r, record -> record))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)), res -> {
            if (res.failed()) {
                resultHandler.handle(Future.failedFuture(res.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture(res.result().stream()
                        .map(CreateResult::new)
                        .collect(toList())));
            }
        });
    }

    default Future<List<CreateResult<E>>> batchCreate(List<E> records) {
        Future<List<CreateResult<E>>> createFuture = Future.future();

        batchCreate(records, createResult -> {
            if (createResult.failed()) {
                createFuture.fail(createResult.cause());
            } else {
                createFuture.complete(createResult.result());
            }
        });

        return createFuture;
    }

    default void update(E record, Handler<AsyncResult<UpdateResult<E>>> resultHandler) {
        batchUpdate(Collections.singletonMap(record, r -> r), res -> doUpdate(resultHandler, res));
    }

    default Future<UpdateResult<E>> update(E record) {
        Future<UpdateResult<E>> updateFuture = Future.future();

        update(record, r -> r, updateResult -> {
            if (updateResult.failed()) {
                updateFuture.fail(updateResult.cause());
            } else {
                updateFuture.complete(updateResult.result());
            }
        });

        return updateFuture;
    }

    default void update(E record, Function<E, E> updateLogic, Handler<AsyncResult<UpdateResult<E>>> resultHandler) {
        batchUpdate(Collections.singletonMap(record, updateLogic), res -> doUpdate(resultHandler, res));
    }

    default void doUpdate(Handler<AsyncResult<UpdateResult<E>>> resultHandler, AsyncResult<List<UpdateResult<E>>> res) {
        if (res.failed()) {
            resultHandler.handle(Future.failedFuture(res.cause()));
        } else {
            Iterator<UpdateResult<E>> iterator = res.result().iterator();

            if (iterator.hasNext()) {
                resultHandler.handle(Future.succeededFuture(iterator.next()));
            } else {
                resultHandler.handle(Future.failedFuture(new NullPointerException()));
            }
        }
    }

    default Future<UpdateResult<E>> update(E record, Function<E, E> updateLogic) {
        Future<UpdateResult<E>> updateFuture = Future.future();

        update(record, updateLogic, updateResult -> {
            if (updateResult.failed()) {
                updateFuture.fail(updateResult.cause());
            } else {
                updateFuture.complete(updateResult.result());
            }
        });

        return updateFuture;
    }

    default void batchUpdate(List<E> records, Handler<AsyncResult<List<UpdateResult<E>>>> resultHandler) {
        Function<E, E> update = rec -> rec;

        final ConcurrentMap<E, Function<E, E>> collect = records.stream()
                .map(r -> new SimpleImmutableEntry<>(r, update))
                .collect(toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));

        batchUpdate(collect, resultHandler);
    }

    default void batchUpdate(Map<E, Function<E, E>> records, Handler<AsyncResult<List<UpdateResult<E>>>> resultHandler) {
        doWrite(false, records, res -> {
            if (res.failed()) {
                resultHandler.handle(Future.failedFuture(res.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture(res.result().stream()
                        .map(UpdateResult::new)
                        .collect(toList())));
            }
        });
    }

    default Future<List<UpdateResult<E>>> batchUpdate(List<E> records) {
        Future<List<UpdateResult<E>>> future = Future.future();

        batchUpdate(records, future.completer());

        return future;
    }

    default Future<List<UpdateResult<E>>> batchUpdate(Map<E, Function<E, E>> records) {
        Future<List<UpdateResult<E>>> updateFuture = Future.future();

        batchUpdate(records, updateResult -> {
            if (updateResult.failed()) {
                updateFuture.fail(updateResult.cause());
            } else {
                updateFuture.complete(updateResult.result());
            }
        });

        return updateFuture;
    }

    default void delete(JsonObject identifiers, Handler<AsyncResult<DeleteResult<E>>> resultHandler) {
        if (identifiers.isEmpty()) {
            resultHandler.handle(ServiceException.fail(400,
                    "Identifier for remoteDelete cannot be empty!"));
        } else {
            batchDelete(Collections.singletonList(identifiers), res -> {
                if (res.failed()) {
                    resultHandler.handle(Future.failedFuture(res.cause()));
                } else {
                    Iterator<DeleteResult<E>> iterator = res.result().iterator();

                    if (iterator.hasNext()) {
                        resultHandler.handle(Future.succeededFuture(iterator.next()));
                    } else {
                        resultHandler.handle(Future.failedFuture(new NullPointerException()));
                    }
                }
            });
        }
    }

    default Future<DeleteResult<E>> delete(JsonObject identifiers) {
        Future<DeleteResult<E>> deleteFuture = Future.future();

        delete(identifiers, deleteResult -> {
            if (deleteResult.failed()) {
                deleteFuture.fail(deleteResult.cause());
            } else {
                deleteFuture.complete(deleteResult.result());
            }
        });

        return deleteFuture;
    }

    default void batchDelete(List<JsonObject> identifiers,
                             Handler<AsyncResult<List<DeleteResult<E>>>> resultHandler) {
        if (identifiers.isEmpty()) {
            resultHandler.handle(ServiceException.fail(400,
                    "Identifiers for batchDelete cannot be empty!"));
        } else {
            doDelete(identifiers, res -> {
                if (res.failed()) {
                    resultHandler.handle(Future.failedFuture(res.cause()));
                } else {
                    resultHandler.handle(Future.succeededFuture(res.result().stream()
                            .map(DeleteResult::new)
                            .collect(toList())));
                }
            });
        }
    }

    default Future<List<DeleteResult<E>>> batchDelete(List<JsonObject> identifiers) {
        Future<List<DeleteResult<E>>> deleteFuture = Future.future();

        batchDelete(identifiers, deleteResult -> {
            if (deleteResult.failed()) {
                deleteFuture.fail(deleteResult.cause());
            } else {
                deleteFuture.complete(deleteResult.result());
            }
        });

        return deleteFuture;
    }

    Function<E, E> incrementField(E record, String fieldName) throws IllegalArgumentException;

    Function<E, E> decrementField(E record, String fieldName) throws IllegalArgumentException;

    void read(JsonObject identifiers, Handler<AsyncResult<ItemResult<E>>> resultHandler);

    void read(JsonObject identifiers, String[] projections, Handler<AsyncResult<ItemResult<E>>> resultHandler);

    default Future<ItemResult<E>> read(JsonObject identifiers) {
        Future<ItemResult<E>> readFuture = Future.future();

        read(identifiers, readResult -> {
            if (readResult.failed()) {
                readFuture.fail(readResult.cause());
            } else {
                readFuture.complete(readResult.result());
            }
        });

        return readFuture;
    }

    default void batchRead(Set<JsonObject> identifiers, Handler<AsyncResult<List<ItemResult<E>>>> resultHandler) {
        batchRead(new ArrayList<>(identifiers), resultHandler);
    }

    default void batchRead(Set<JsonObject> identifiers, String[] projections,
                           Handler<AsyncResult<List<ItemResult<E>>>> resultHandler) {
        batchRead(new ArrayList<>(identifiers), resultHandler);
    }

    default void batchRead(List<JsonObject> identifiers,
                           Handler<AsyncResult<List<ItemResult<E>>>> resultHandler) {
        batchRead(new ArrayList<>(identifiers), null, resultHandler);
    }

    @SuppressWarnings("SimplifyStreamApiCallChains")
    default void batchRead(List<JsonObject> identifiers, String[] projections,
                           Handler<AsyncResult<List<ItemResult<E>>>> resultHandler) {
        List<Future> futureList = new ArrayList<>();
        Queue<Future<ItemResult<E>>> queuedFutures = new ConcurrentLinkedQueue<>();

        identifiers.stream().forEachOrdered(identifier -> {
            Future<ItemResult<E>> future = Future.future();
            futureList.add(future);
            queuedFutures.add(future);

            if (projections != null) {
                read(identifier, projections, future.completer());
            } else {
                read(identifier, future.completer());
            }
        });

        CompositeFuture.all(futureList).setHandler(res -> {
            if (res.failed()) {
                resultHandler.handle(ServiceException.fail(500, "Unable to performed batchread!",
                        new JsonObject().put("ids", identifiers)));
            } else {
                List<ItemResult<E>> results = queuedFutures.stream()
                        .map(Future::result)
                        .collect(toList());

                resultHandler.handle(Future.succeededFuture(results));
            }
        });
    }

    default Future<List<ItemResult<E>>> batchRead(List<JsonObject> identifiers) {
        return batchRead(identifiers, (String[]) null);
    }

    default Future<List<ItemResult<E>>> batchRead(List<JsonObject> identifiers, String[] projections) {
        Future<List<ItemResult<E>>> future = Future.future();

        batchRead(identifiers, projections, future.completer());

        return future;
    }

    default void read(JsonObject identifiers, boolean consistent, Handler<AsyncResult<ItemResult<E>>> resultHandler) {
        read(identifiers, consistent, null, resultHandler);
    }

    default Future<ItemResult<E>> read(JsonObject identifiers, boolean consistent) {
        Future<ItemResult<E>> readFuture = Future.future();

        read(identifiers, consistent, readResult -> {
            if (readResult.failed()) {
                readFuture.fail(readResult.cause());
            } else {
                readFuture.complete(readResult.result());
            }
        });

        return readFuture;
    }

    void read(JsonObject identifiers, boolean consistent, String[] projections, Handler<AsyncResult<ItemResult<E>>> resultHandler);

    default Future<ItemResult<E>> read(JsonObject identifiers, boolean consistent, String[] projections) {
        Future<ItemResult<E>> readFuture = Future.future();

        read(identifiers, consistent, projections, readResult -> {
            if (readResult.failed()) {
                readFuture.fail(readResult.cause());
            } else {
                readFuture.complete(readResult.result());
            }
        });

        return readFuture;
    }

    void readAll(Handler<AsyncResult<List<E>>> resultHandler);

    default Future<List<E>> readAll() {
        Future<List<E>> readFuture = Future.future();

        readAll(readAllResult -> {
            if (readAllResult.failed()) {
                readFuture.fail(readAllResult.cause());
            } else {
                readFuture.complete(readAllResult.result());
            }
        });

        return readFuture;
    }

    default void readAll(String pageToken, Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        readAll(null, pageToken, null, null, resultHandler);
    }

    default Future<ItemListResult<E>> readAll(String pageToken) {
        Future<ItemListResult<E>> readFuture = Future.future();

        readAll(null, pageToken, null, null, readAllResult -> {
            if (readAllResult.failed()) {
                readFuture.fail(readAllResult.cause());
            } else {
                readFuture.complete(readAllResult.result());
            }
        });

        return readFuture;
    }

    void readAll(JsonObject identifiers, Map<String, List<FilterParameter>> filterParameterMap,
                 Handler<AsyncResult<List<E>>> resultHandler);

    default Future<List<E>> readAll(JsonObject identifiers, Map<String, List<FilterParameter>> filterParamterMap) {
        Future<List<E>> readFuture = Future.future();

        readAll(identifiers, filterParamterMap, readAllResult -> {
            if (readAllResult.failed()) {
                readFuture.fail(readAllResult.cause());
            } else {
                readFuture.complete(readAllResult.result());
            }
        });

        return readFuture;
    }

    default void readAll(JsonObject identifiers, QueryPack queryPack,
                         Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        readAll(identifiers, queryPack.getPageToken(), queryPack, queryPack.getProjections(), resultHandler);
    }

    default Future<ItemListResult<E>> readAll(JsonObject identifiers, QueryPack queryPack) {
        Future<ItemListResult<E>> future = Future.future();

        readAll(identifiers, queryPack.getPageToken(), queryPack, queryPack.getProjections(), future.completer());

        return future;
    }

    void readAll(JsonObject identifiers, String pageToken, QueryPack queryPack, String[] projections,
                 Handler<AsyncResult<ItemListResult<E>>> resultHandler);

    default Future<ItemListResult<E>> readAll(JsonObject identifiers, String pageToken, 
                                              QueryPack queryPack, String[] projections) {
        Future<ItemListResult<E>> readFuture = Future.future();

        readAll(identifiers, pageToken, queryPack, projections, readAllResult -> {
            if (readAllResult.failed()) {
                readFuture.fail(readAllResult.cause());
            } else {
                readFuture.complete(readAllResult.result());
            }
        });

        return readFuture;
    }

    void readAll(String pageToken, QueryPack queryPack, String[] projections,
                 Handler<AsyncResult<ItemListResult<E>>> resultHandler);

    default Future<ItemListResult<E>> readAll(String pageToken,
                                              QueryPack queryPack, String[] projections) {
        Future<ItemListResult<E>> readFuture = Future.future();

        readAll(pageToken, queryPack, projections, readAllResult -> {
            if (readAllResult.failed()) {
                readFuture.fail(readAllResult.cause());
            } else {
                readFuture.complete(readAllResult.result());
            }
        });

        return readFuture;
    }

    default void aggregation(JsonObject identifiers, QueryPack queryPack, Handler<AsyncResult<String>> resultHandler) {
        aggregation(identifiers, queryPack, queryPack.getProjections(), resultHandler);
    }

    default Future<String> aggregation(JsonObject identifiers, QueryPack queryPack) {
        Future<String> readFuture = Future.future();

        aggregation(identifiers, queryPack, readFuture.completer());

        return readFuture;
    }

    void aggregation(JsonObject identifiers, QueryPack queryPack, String[] projections,
                     Handler<AsyncResult<String>> resultHandler);

    default Future<String> aggregation(JsonObject identifiers, QueryPack queryPack, String[] projections) {
        Future<String> readFuture = Future.future();

        aggregation(identifiers, queryPack, projections, readAllResult -> {
            if (readAllResult.failed()) {
                readFuture.fail(readAllResult.cause());
            } else {
                readFuture.complete(readAllResult.result());
            }
        });

        return readFuture;
    }

    JsonObject buildParameters(Map<String, List<String>> queryMap,
                               Field[] fields, Method[] methods,
                               JsonObject errors,
                               Map<String, List<FilterParameter>> params, int[] limit,
                               Queue<OrderByParameter> orderByQueue,
                               String[] indexName);

    @SuppressWarnings("unchecked")
    default void parseParam(Class<E> type, String paramJsonString, String key,
                            Map<String, List<FilterParameter>> params, JsonObject errors) {
        FilterParameter filterParameters = Json.decodeValue(paramJsonString, FilterParameter.class);

        if (filterParameters != null) {
            filterParameters.setField(key);

            if (filterParameters.isValid()) {
                List<FilterParameter> filterParameterList = params.get(key);
                if (filterParameterList == null) filterParameterList = new ArrayList<>();
                filterParameterList.add(filterParameters);
                params.put(key, filterParameterList);
            } else {
                filterParameters.collectErrors(errors);
            }
        } else {
            errors.put(key + "_error", "Unable to parse JSON in '" + key + "' value!");
        }
    }

    void readAllWithoutPagination(String identifier, Handler<AsyncResult<List<E>>> resultHandler);

    default Future<List<E>> readAllWithoutPagination(String identifier) {
        Future<List<E>> readFuture = Future.future();

        readAllWithoutPagination(identifier, readAllResult -> {
            if (readAllResult.failed()) {
                readFuture.fail(readAllResult.cause());
            } else {
                readFuture.complete(readAllResult.result());
            }
        });

        return readFuture;
    }

    void readAllWithoutPagination(String identifier, QueryPack queryPack, Handler<AsyncResult<List<E>>> resultHandler);

    default Future<List<E>> readAllWithoutPagination(String identifier, QueryPack queryPack) {
        Future<List<E>> readFuture = Future.future();

        readAllWithoutPagination(identifier, queryPack, readAllResult -> {
            if (readAllResult.failed()) {
                readFuture.fail(readAllResult.cause());
            } else {
                readFuture.complete(readAllResult.result());
            }
        });

        return readFuture;
    }

    void readAllWithoutPagination(String identifier, QueryPack queryPack, String[] projections, Handler<AsyncResult<List<E>>> resultHandler);

    default Future<List<E>> readAllWithoutPagination(String identifier, QueryPack queryPack, String[] projections) {
        Future<List<E>> readFuture = Future.future();

        readAllWithoutPagination(identifier, queryPack, projections, readAllResult -> {
            if (readAllResult.failed()) {
                readFuture.fail(readAllResult.cause());
            } else {
                readFuture.complete(readAllResult.result());
            }
        });

        return readFuture;
    }

    default void readAllWithoutPagination(QueryPack queryPack, Handler<AsyncResult<List<E>>> resultHandler) {
        readAllWithoutPagination(queryPack, null, resultHandler);
    }

    default Future<List<E>> readAllWithoutPagination(QueryPack queryPack) {
        return readAllWithoutPagination(queryPack, (String[]) null);
    }

    void readAllWithoutPagination(QueryPack queryPack, String[] projections, Handler<AsyncResult<List<E>>> resultHandler);

    default Future<List<E>> readAllWithoutPagination(QueryPack queryPack, String[] projections) {
        Future<List<E>> readFuture = Future.future();

        readAllWithoutPagination(queryPack, projections, readAllResult -> {
            if (readAllResult.failed()) {
                readFuture.fail(readAllResult.cause());
            } else {
                readFuture.complete(readAllResult.result());
            }
        });

        return readFuture;
    }

    void doWrite(boolean create, Map<E, Function<E, E>> records, Handler<AsyncResult<List<E>>> resultHandler);

    @SuppressWarnings("unchecked")
    default E setCreatedAt(E record) {
        return (E) record.setCreatedAt(new Date());
    }

    @SuppressWarnings("unchecked")
    default E setUpdatedAt(E record) {
        return (E) record.setUpdatedAt(new Date());
    }

    void doDelete(List<JsonObject> identifiers, Handler<AsyncResult<List<E>>> resultHandler);

    default String buildCollectionEtag(List<String> etags) {
        final long[] newEtag = new long[1];

        try {
            etags.forEach(etag -> newEtag[0] += etag.hashCode());

            return ModelUtils.hashString(String.valueOf(newEtag[0]));
        } catch (NoSuchAlgorithmException | NullPointerException e) {
            e.printStackTrace();

            return "noTagForCollection";
        }
    }

    default Double extractValueAsDouble(Field field, E r) {
        try {
            return (double) field.getLong(r);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            try {
                return ((Long) field.get(r)).doubleValue();
            } catch (IllegalArgumentException | IllegalAccessException ignored) {}
        }

        try {
            return (double) field.getInt(r);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            try {
                return ((Integer) field.get(r)).doubleValue();
            } catch (IllegalArgumentException | IllegalAccessException ignored) {}
        }

        try {
            return (double) field.getShort(r);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            try {
                return ((Short) field.get(r)).doubleValue();
            } catch (IllegalArgumentException | IllegalAccessException ignored) {}
        }

        try {
            return field.getDouble(r);
        } catch (IllegalArgumentException | IllegalAccessException ignored) {}

        try {
            return (double) field.getFloat(r);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            try {
                return ((Float) field.get(r)).doubleValue();
            } catch (IllegalArgumentException | IllegalAccessException ignored) {}
        }

        logger.error("Conversion is null!");

        return null;
    }

    ETagManager getEtagManager();

    boolean isCached();
    boolean isEtagEnabled();
}
