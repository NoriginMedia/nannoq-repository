package com.nannoq.tools.repository.repository;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedParallelScanList;
import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.DynamoDBModel;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.nannoq.tools.repository.repository.RedisUtils.performJedisWithRetry;
import static java.util.stream.Collectors.toList;

public class ETagManager<E extends ETagable & DynamoDBModel & Model & Cacheable> {
    private static final Logger logger = LoggerFactory.getLogger(CacheManager.class.getSimpleName());

    private final Class<E> TYPE;
    private final Vertx vertx;

    private final String COLLECTION;
    private final DynamoDBMapper DYNAMO_DB_MAPPER;
    private final RedisClient REDIS_CLIENT;

    public ETagManager(Class<E> type, Vertx vertx, String COLLECTION, DynamoDBRepository<E> db) {
        TYPE = type;
        this.vertx = vertx;
        this.COLLECTION = COLLECTION;
        this.REDIS_CLIENT = db.getRedisClient();
        this.DYNAMO_DB_MAPPER = db.getDynamoDbMapper();
    }

    public String buildCollectionEtagKey() {
        return "data_api_" + COLLECTION + "_s_etag";
    }

    @SuppressWarnings("unchecked")
    public void getEtags(Handler<AsyncResult<List<String>>> resultHandler) {
        vertx.executeBlocking(future -> {
            try {
                final DynamoDBScanExpression exp = new DynamoDBScanExpression()
                        .withProjectionExpression("etag")
                        .withSelect("SPECIFIC_ATTRIBUTES");

                PaginatedParallelScanList<E> items = DYNAMO_DB_MAPPER
                        .parallelScan(TYPE, exp, Runtime.getRuntime().availableProcessors() * 2);
                items.loadAllResults();

                future.complete(items);
            } catch (AmazonServiceException ase) {
                logger.error("Could not complete DynamoDB Operation, " +
                        "Error Message:  " + ase.getMessage() + ", " +
                        "HTTP Status:    " + ase.getStatusCode() + ", " +
                        "AWS Error Code: " + ase.getErrorCode() + ", " +
                        "Error Type:     " + ase.getErrorType() + ", " +
                        "Request ID:     " + ase.getRequestId());

                future.fail(ase);
            } catch (AmazonClientException ace) {
                logger.error("Internal Dynamodb Error, " + "Error Message:  " + ace.getMessage());

                future.fail(ace);
            } catch (Exception e) {
                logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));

                future.fail(e);
            }
        }, false, readResult -> {
            if (readResult.failed()) {
                logger.error("Could not fetch records for etags...");

                resultHandler.handle(Future.succeededFuture(new ArrayList<>()));
            } else {
                resultHandler.handle(Future.succeededFuture(
                        ((List<E>) readResult.result()).parallelStream()
                                .map(ETagable::getEtag)
                                .collect(toList())));
            }
        });
    }

    void removeProjectionsEtags(String hash, Future<Boolean> future) {
        String etagKeyBase = TYPE.getSimpleName() + "_" + hash + "/projections";

        performJedisWithRetry(REDIS_CLIENT, in -> in.hgetall(etagKeyBase, allRes -> {
            if (allRes.failed()) {
                future.complete(Boolean.TRUE);
            } else {
                JsonObject result = allRes.result();
                List<String> itemsToRemove = new ArrayList<>();
                result.iterator().forEachRemaining(item -> itemsToRemove.add(item.getKey()));

                performJedisWithRetry(REDIS_CLIENT, inner ->
                        inner.hdelMany(etagKeyBase, itemsToRemove, manyDelRes -> {
                            future.complete(Boolean.TRUE);
                        }));
            }
        }));
    }

    void destroyEtags(String hash, Future<Boolean> future) {
        String etagItemListHashKey = TYPE.getSimpleName() + "_" +
                (hash != null ? hash + "_" : "") +
                "itemListEtags";

        performJedisWithRetry(REDIS_CLIENT, in -> in.hgetall(etagItemListHashKey, allRes -> {
            if (allRes.failed()) {
                future.complete(Boolean.TRUE);
            } else {
                JsonObject result = allRes.result();
                List<String> itemsToRemove = new ArrayList<>();
                result.iterator().forEachRemaining(item -> itemsToRemove.add(item.getKey()));

                performJedisWithRetry(REDIS_CLIENT, inner ->
                        inner.hdelMany(etagItemListHashKey, itemsToRemove, manyDelRes -> {
                            future.complete(Boolean.TRUE);
                        }));
            }
        }));
    }

    public void replaceAggEtag(String etagItemListHashKey, String etagKey, String newEtag,
                               Handler<AsyncResult<Boolean>> resultHandler) {
        performJedisWithRetry(REDIS_CLIENT, ir -> ir.hset(etagItemListHashKey, etagKey, newEtag, setRes -> {
            if (setRes.failed()) {
                resultHandler.handle(Future.failedFuture(setRes.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
            }
        }));
    }
}
