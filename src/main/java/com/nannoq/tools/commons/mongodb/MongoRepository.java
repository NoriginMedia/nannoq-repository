package com.nannoq.tools.repository.mongodb;

import com.nannoq.tools.repository.RedisUtils;
import com.nannoq.tools.repository.Repository;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.utils.FilterParameter;
import com.nannoq.tools.repository.utils.ItemList;
import com.nannoq.tools.repository.utils.OrderByParameter;
import com.nannoq.tools.repository.utils.QueryPack;
import de.braintags.io.vertx.pojomapper.dataaccess.delete.IDelete;
import de.braintags.io.vertx.pojomapper.dataaccess.query.IQuery;
import de.braintags.io.vertx.pojomapper.dataaccess.query.IQueryResult;
import de.braintags.io.vertx.pojomapper.mongo.MongoDataStore;
import de.braintags.io.vertx.util.IteratorAsync;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.redis.RedisClient;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Created by anders on 05/08/16.
 */
public class MongoRepository<E extends ETagable & Model> implements Repository<E> {
    private static final Logger logger = LoggerFactory.getLogger(MongoRepository.class.getSimpleName());

    private final String COLLECTION;
    final Class<E> TYPE;
    private final String IDENTIFIER;
    final MongoDataStore MONGO_DATA_STORE;
    private final RedisClient REDIS_CLIENT;

    MongoRepository(Vertx vertx, MongoDataStore mongoDataStore, Class<E> type,
                    String collection, String identifier, JsonObject appConfig) {
        this.MONGO_DATA_STORE = mongoDataStore;
        this.TYPE = type;
        this.COLLECTION = collection;
        this.IDENTIFIER = identifier;
        this.REDIS_CLIENT = RedisUtils.getRedisClient(vertx, appConfig);
    }

    @Override
    public void read(JsonObject identifiers, Handler<AsyncResult<E>> resultHandler) {

    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(JsonObject identifiers, boolean consistent, String[] projections,
                     Handler<AsyncResult<E>> resultHandler) {
        IQuery<E> showQuery = MONGO_DATA_STORE.createQuery(TYPE);
        showQuery.field(IDENTIFIER).is(identifiers.getString("id"));

        showQuery.execute(result -> {
            if (result.failed()) {
                logger.error(result.cause());
            } else {
                IQueryResult<E> queryResult = result.result();
                IteratorAsync<E> it = queryResult.iterator();

                if (it.hasNext()) {
                    it.next(itResult -> {
                        E item = null;

                        if (itResult.failed()) {
                            logger.error("No item found...");
                            logger.error(itResult.cause());
                        } else {
                            item = itResult.result();
                        }

                        resultHandler.handle(Future.succeededFuture(item));
                    });
                } else {
                    resultHandler.handle(Future.failedFuture(new NoSuchElementException()));
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    void readIterator(List<E> items, IteratorAsync<E> it,
                      int size, Handler<AsyncResult<List<E>>> resultHandler) {
        List<E> finalItems = items != null ? items : new ArrayList<>();
        List<Future> futures = new ArrayList<>();

        IntStream.range(0, size).forEach(i -> {
            Future future = Future.future();
            Handler<AsyncResult<E>> iterationHandler = result -> finalItems.add(result.result());

            future.setHandler(iterationHandler);
            futures.add(future);

            it.next(future.completer());
        });

        CompositeFuture.all(futures).setHandler(allDone -> {
            if (allDone.failed()) {
                logger.error("Iterationcollection failed...");

                resultHandler.handle(Future.succeededFuture(new ArrayList<>()));
            } else {
                resultHandler.handle(Future.succeededFuture(finalItems));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readAll(Handler<AsyncResult<List<E>>> resultHandler) {
        IQuery<E> indexQuery = MONGO_DATA_STORE.createQuery(TYPE);
        indexQuery.execute(results -> {
            if (results.failed()) {
                logger.error("Index fetch failed...");
                logger.error(results.cause());
            } else {
                IQueryResult<E> queryResult = results.result();
                IteratorAsync<E> it = queryResult.iterator();

                if (it.hasNext()) {
                    resultHandler.handle(Future.succeededFuture(new ArrayList<>()));
                } else {
                    readIterator(null, it, queryResult.size(), resultHandler);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readAll(String pageToken, Handler<AsyncResult<ItemList<E>>> resultHandler) {
//        IQuery<E> indexQuery = MONGO_DATA_STORE.createQuery(TYPE);
//        indexQuery.execute(results -> {
//            if (results.failed()) {
//                logger.error("Index fetch failed...");
//                logger.error(results.cause());
//            } else {
//                IQueryResult<E> queryResult = results.result();
//                IteratorAsync<E> it = queryResult.iterator();
//
//                if (it.hasNext()) {
//                    resultHandler.handle(new ETagableListResult(new ArrayList<>()));
//                } else {
//                    readIterator(null, it, queryResult.size(), resultHandler);
//                }
//            }
//        });
    }

    @Override
    public void readAll(JsonObject identifiers, Map<String, List<FilterParameter<E>>> filterParameterMap,
                        Handler<AsyncResult<List<E>>> resultHandler) {

    }

    @Override
    public void readAll(JsonObject identifiers, String pageToken, QueryPack<E> queryPack, String[] projections,
                        Handler<AsyncResult<ItemList<E>>> resultHandler) {

    }

    @Override
    public void aggregation(JsonObject identifiers, QueryPack<E> queryPack, String[] projections,
                            Handler<AsyncResult<String>> resultHandler) {

    }

    @Override
    public JsonObject buildParameters(Map<String, List<String>> queryMap, Field[] fields, Method[] methods,
                                      JsonObject errors, Map<String, List<FilterParameter<E>>> params, int[] limit,
                                      Queue<OrderByParameter> orderByQueue, String[] indexName) {
        return null;
    }

    @Override
    public void readAllWithoutPagination(String identifier, Handler<AsyncResult<List<E>>> resultHandler) {

    }

    @Override
    public void readAllWithoutPagination(String identifier, QueryPack<E> queryPack, Handler<AsyncResult<List<E>>> resultHandler) {

    }

    @Override
    public void readAllWithoutPagination(String identifier, QueryPack<E> queryPack, String[] projections, Handler<AsyncResult<List<E>>> resultHandler) {

    }

    @Override
    public void readAllWithoutPagination(QueryPack<E> queryPack, String[] projections, Handler<AsyncResult<List<E>>> resultHandler) {

    }

    @Override
    public boolean incrementField(E record, String fieldName) throws IllegalArgumentException {
        return false;
    }

    @Override
    public boolean decrementField(E record, String fieldName) throws IllegalArgumentException {
        return false;
    }

    @Override
    public void doDelete(List<JsonObject> identifiers, Handler<AsyncResult<List<E>>> resultHandler) {
        IQuery<E> idQuery = MONGO_DATA_STORE.createQuery(TYPE);
        idQuery.field(IDENTIFIER).is(identifiers.stream()
                .map(id -> id.getString("id"))
                .collect(toList()));
        IDelete<E> deleter = MONGO_DATA_STORE.createDelete(TYPE);
        deleter.setQuery(idQuery);

        deleter.delete(result -> {
            if (result.succeeded()) {
                setCollectionEtag(tagResult ->
                        RedisUtils.performJedisWithRetry(REDIS_CLIENT, tagResult.result()));
            } else {
                logger.error("Delete failed...");
                logger.error(result.cause());
            }

            //resultHandler.handle(Future.succeededFuture(result.succeeded()));
        });
    }

    @Override
    public void doWrite(boolean create, Map<E, Function<E, E>> records, Handler<AsyncResult<List<E>>> resultHandler) {
//        IWrite<E> write = MONGO_DATA_STORE.createWrite(TYPE);
//
//        records.parallelStream().forEach(record -> {
//            setSingleRecordEtag(record.generateAndSetEtag(new HashMap<>()), tagResult ->
//                    RedisUtils.performJedisWithRetry(REDIS_CLIENT, tagResult.result()));
//            write.add(record);
//        });
//
//        write.save(result -> {
//            if (result.succeeded()) {
//                setCollectionEtag(tagResult ->
//                        RedisUtils.performJedisWithRetry(REDIS_CLIENT, tagResult.result()));
//            } else {
//                logger.error("Write failed...");
//                logger.error(result.cause());
//            }
//
//            resultHandler.handle(Future.succeededFuture(result.succeeded()));
//        });
    }

    @Override
    public void getEtags(Handler<AsyncResult<List<String>>> resultHandler) {
        IQuery<E> indexQuery = MONGO_DATA_STORE.createQuery(TYPE);

        indexQuery.execute(results -> {
            if (results.failed()) {
                logger.error("Index for etags failed...");
                logger.error(results.cause());
            } else {
                IQueryResult<E> queryResult = results.result();
                IteratorAsync<E> it = queryResult.iterator();

                readIterator(null, it, queryResult.size(), result -> {
                    if (result.failed()) {
                        logger.error("Could not get etag items...");
                    } else {
                        resultHandler.handle(new AsyncResult<List<String>>() {
                            @Override
                            public List<String> result() {
                                return result.result().parallelStream()
                                        .map(ETagable::getEtag)
                                        .collect(toList());
                            }

                            @Override
                            public Throwable cause() {
                                return results.cause();
                            }

                            @Override
                            public boolean succeeded() {
                                return results.succeeded();
                            }

                            @Override
                            public boolean failed() {
                                return results.failed();
                            }
                        });
                    }
                });
            }
        });
    }

    @Override
    public String buildCollectionEtagKey() {
        return "data_api_" + COLLECTION + "_s_etag";
    }

    @SuppressWarnings("unchecked")
    public static <E extends ETagable> MongoRepository getMongoStore(
            Vertx vertx, JsonObject config, Class<E> type, String col, String identifier) {
        MongoDataStore mongoDataStore =
                new MongoDataStore(vertx, MongoClient.createShared(vertx, config), config);

        return new MongoRepository(vertx, mongoDataStore, type, col, identifier, config);
    }
}
