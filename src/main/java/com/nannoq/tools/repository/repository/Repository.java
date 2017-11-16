package com.nannoq.tools.repository.repository;

import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.models.ModelUtils;
import com.nannoq.tools.repository.repository.results.*;
import com.nannoq.tools.repository.utils.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;
import io.vertx.serviceproxy.ServiceException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.AbstractMap.SimpleEntry;
import static java.util.stream.Collectors.toList;

/**
 * Created by anders on 05/08/16.
 */
@SuppressWarnings("unused")
public interface Repository<E extends ETagable & Model> {
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

    default void update(E record, Function<E, E> updateLogic, Handler<AsyncResult<UpdateResult<E>>> resultHandler) {
        batchUpdate(Collections.singletonMap(record, updateLogic), res -> {
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
        });
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

    boolean incrementField(E record, String fieldName) throws IllegalArgumentException;

    boolean decrementField(E record, String fieldName) throws IllegalArgumentException;

    void read(JsonObject identifiers, Handler<AsyncResult<ItemResult<E>>> resultHandler);

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

    @SuppressWarnings("SimplifyStreamApiCallChains")
    default void batchRead(List<JsonObject> identifiers, Handler<AsyncResult<List<ItemResult<E>>>> resultHandler) {
        List<Future> futureList = new ArrayList<>();
        Queue<Future<ItemResult<E>>> queuedFutures = new ConcurrentLinkedQueue<>();

        identifiers.stream().forEachOrdered(identifier -> {
            Future<ItemResult<E>> future = Future.future();
            futureList.add(future);
            queuedFutures.add(future);

            read(identifier, future.completer());
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

    void readAll(JsonObject identifiers, Map<String, List<FilterParameter<E>>> filterParameterMap,
                 Handler<AsyncResult<List<E>>> resultHandler);

    default Future<List<E>> readAll(JsonObject identifiers,
                                    Map<String, List<FilterParameter<E>>> filterParamterMap) {
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

    void readAll(JsonObject identifiers, String pageToken, QueryPack<E> queryPack, String[] projections,
                 Handler<AsyncResult<ItemListResult<E>>> resultHandler);

    default Future<ItemListResult<E>> readAll(JsonObject identifiers, String pageToken,
                                        QueryPack<E> queryPack, String[] projections) {
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

    void readAll(String pageToken, QueryPack<E> queryPack, String[] projections,
                 Handler<AsyncResult<ItemListResult<E>>> resultHandler);

    default Future<ItemListResult<E>> readAll(String pageToken,
                                              QueryPack<E> queryPack, String[] projections) {
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

    void aggregation(JsonObject identifiers, QueryPack<E> queryPack, String[] projections,
                     Handler<AsyncResult<String>> resultHandler);

    default Future<String> aggregation(JsonObject identifiers, QueryPack<E> queryPack, String[] projections) {
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
                               Map<String, List<FilterParameter<E>>> params, int[] limit,
                               Queue<OrderByParameter> orderByQueue,
                               String[] indexName);

    @SuppressWarnings("unchecked")
    default void parseParam(Class<E> type, String paramJsonString, String key,
                            Map<String, List<FilterParameter<E>>> params, JsonObject errors) {
        FilterParameter<E> filterParameters = Json.decodeValue(paramJsonString, FilterParameter.class);

        if (filterParameters != null) {
            filterParameters.setField(key);
            filterParameters.setClassType(type);

            if (filterParameters.isValid()) {
                List<FilterParameter<E>> filterParameterList = params.get(key);
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

    void readAllWithoutPagination(String identifier, QueryPack<E> queryPack, Handler<AsyncResult<List<E>>> resultHandler);

    default Future<List<E>> readAllWithoutPagination(String identifier, QueryPack<E> queryPack) {
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

    void readAllWithoutPagination(String identifier, QueryPack<E> queryPack, String[] projections, Handler<AsyncResult<List<E>>> resultHandler);

    default Future<List<E>> readAllWithoutPagination(String identifier, QueryPack<E> queryPack, String[] projections) {
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

    default void readAllWithoutPagination(QueryPack<E> queryPack, Handler<AsyncResult<List<E>>> resultHandler) {
        readAllWithoutPagination(queryPack, null, resultHandler);
    }

    default Future<List<E>> readAllWithoutPagination(QueryPack<E> queryPack) {
        return readAllWithoutPagination(queryPack, (String[]) null);
    }

    void readAllWithoutPagination(QueryPack<E> queryPack, String[] projections, Handler<AsyncResult<List<E>>> resultHandler);

    default Future<List<E>> readAllWithoutPagination(QueryPack<E> queryPack, String[] projections) {
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

    String buildCollectionEtagKey();

    default void setCollectionEtag(Handler<AsyncResult<Consumer<RedisClient>>> resultHandler) {
        String collectionEtagKey = buildCollectionEtagKey();

        getEtags(result -> {
            if (result.failed()) {
                logger.error("Could not build etags...");
                logger.error(result.cause());
            } else {
                String collectionEtag = buildCollectionEtag(result.result());

                Consumer<RedisClient> consumer = redisClient ->
                        redisClient.set(collectionEtagKey, collectionEtag, redisResult -> {
                            if (redisResult.failed()) {
                                logger.error("Could not set " + collectionEtag +
                                        " for " + collectionEtagKey + ",cause: " + result.cause());
                            }
                        });

                resultHandler.handle(Future.succeededFuture(consumer));
            }
        });
    }

    void getEtags(Handler<AsyncResult<List<String>>> resultHandler);

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

    default void setSingleRecordEtag(Map<String, String> etagMap,
                                     Handler<AsyncResult<Consumer<RedisClient>>> resultHandler) {
        Consumer<RedisClient> consumer = redisClient -> etagMap.keySet()
                .forEach(key -> redisClient.set(key, etagMap.get(key), result -> {
                    if (result.failed()) {
                        logger.error("Could not set " + etagMap.get(key) + " for " + key + ",cause: " + result.cause());
                    }
                }));

        resultHandler.handle(Future.succeededFuture(consumer));
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
}
