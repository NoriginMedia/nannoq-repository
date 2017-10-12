package com.nannoq.tools.repository.dynamodb.operators;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDeleteExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.KeyPair;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.google.common.collect.ImmutableMap;
import com.nannoq.tools.repository.CacheManager;
import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.DynamoDBModel;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ServiceException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class DynamoDBDeleter<E extends DynamoDBModel & Model & ETagable & Cacheable> {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBDeleter.class.getSimpleName());

    private final Class<E> TYPE;
    private final Vertx vertx;
    private final DynamoDBRepository<E> db;
    private final CacheManager<E> cacheManager;

    private final String HASH_IDENTIFIER;
    private final String IDENTIFIER;

    private final DynamoDBMapper DYNAMO_DB_MAPPER;

    public DynamoDBDeleter(Class<E> type, Vertx vertx, DynamoDBRepository<E> db,
                           String HASH_IDENTIFIER, String IDENTIFIER,
                           CacheManager<E> cacheManager) {
        TYPE = type;
        this.vertx = vertx;
        this.db = db;
        this.cacheManager = cacheManager;
        this.DYNAMO_DB_MAPPER = db.getDynamoDbMapper();
        this.HASH_IDENTIFIER = HASH_IDENTIFIER;
        this.IDENTIFIER = IDENTIFIER;
    }

    @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
    public void doDelete(List<JsonObject> identifiers, Handler<AsyncResult<List<E>>> resultHandler) {
        vertx.<List<E>>executeBlocking(future -> {
            try {
                List<E> items = DYNAMO_DB_MAPPER.batchLoad(Collections.singletonMap(TYPE, identifiers.stream()
                        .map(id -> new KeyPair()
                                .withHashKey(id.getString("hash"))
                                .withRangeKey(id.getString("range")))
                        .collect(toList()))).entrySet().iterator().next().getValue().stream()
                        .map(item -> (E) item)
                        .collect(toList());

                if (logger.isDebugEnabled()) { logger.debug("To Delete: " + Json.encodePrettily(items)); }

                List<Future> deleteFutures = new ArrayList<>();

                items.forEach(record -> {
                    Future<E> deleteFuture = Future.future();

                    try {
                        this.optimisticLockingDelete(record, null, deleteFuture);
                    } catch (Exception e) {
                        logger.error(e);

                        deleteFuture.fail(e);
                    }

                    deleteFutures.add(deleteFuture);
                });

                CompositeFuture.all(deleteFutures).setHandler(res -> {
                    if (res.failed()) {
                        future.fail(res.cause());
                    } else {
                        Future<Boolean> purgeFuture = Future.future();
                        purgeFuture.setHandler(purgeRes -> {
                            if (purgeRes.failed()) {
                                future.fail(purgeRes.cause());
                            } else {
                                future.complete(deleteFutures.stream()
                                        .map(finalFuture -> (E) finalFuture.result())
                                        .collect(toList()));
                            }
                        });

                        cacheManager.purgeCache(purgeFuture, items, e -> {
                            String hash = e.getHash();
                            String range = e.getRange();

                            return TYPE.getSimpleName() + "_" + hash + (range.equals("") ? "" : "/" + range);
                        });
                    }
                });
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
        }, false, result -> {
            if (result.failed()) {
                resultHandler.handle(ServiceException.fail(500, "Unable to perform remoteDelete!",
                        new JsonObject(Json.encode(result.cause()))));
            } else {
                resultHandler.handle(Future.succeededFuture(result.result()));
            }
        });
    }

    private void optimisticLockingDelete(E record, Integer prevCounter, Future<E> deleteFuture) {
        Integer counter = 0;
        if (prevCounter != null) counter = prevCounter;

        try {
            DYNAMO_DB_MAPPER.delete(record, buildExistingDeleteExpression(record));

            deleteFuture.complete(record);
        } catch (ConditionalCheckFailedException e) {
            logger.error("DeleteCollision on: " +
                    record.getClass().getSimpleName() + " : " + record.getHash() + " : " + record.getRange() + ", " +
                    "Error Message:  " + e.getMessage() + ", " +
                    "HTTP Status:    " + e.getStatusCode() + ", " +
                    "AWS Error Code: " + e.getErrorCode() + ", " +
                    "Error Type:     " + e.getErrorType() + ", " +
                    "Request ID:     " + e.getRequestId() + ", " +
                    ", retrying...");

            if (counter > 100) {
                logger.error(Json.encodePrettily(record));

                throw new InternalError();
            }

            E newestRecord = db.fetchNewestRecord(TYPE, record.getHash(), record.getRange());

            optimisticLockingDelete(newestRecord, ++counter, deleteFuture);
        } catch (AmazonServiceException ase) {
            logger.error("Could not complete DynamoDB Operation, " +
                    "Error Message:  " + ase.getMessage() + ", " +
                    "HTTP Status:    " + ase.getStatusCode() + ", " +
                    "AWS Error Code: " + ase.getErrorCode() + ", " +
                    "Error Type:     " + ase.getErrorType() + ", " +
                    "Request ID:     " + ase.getRequestId());

            if (counter > 100) {
                logger.error(Json.encodePrettily(record));

                throw new InternalError();
            }

            E newestRecord = db.fetchNewestRecord(TYPE, record.getHash(), record.getRange());

            optimisticLockingDelete(newestRecord, ++counter, deleteFuture);
        } catch (AmazonClientException ace) {
            logger.error("Internal Dynamodb Error, " + "Error Message:  " + ace.getMessage());

            if (counter > 100) {
                logger.error(Json.encodePrettily(record));

                throw new InternalError();
            }

            E newestRecord = db.fetchNewestRecord(TYPE, record.getHash(), record.getRange());

            optimisticLockingDelete(newestRecord, ++counter, deleteFuture);
        }
    }

    private DynamoDBDeleteExpression buildExistingDeleteExpression(E element) {
        ImmutableMap.Builder<String, ExpectedAttributeValue> expectationbuilder =
                new ImmutableMap.Builder<String, ExpectedAttributeValue>()
                        .put(HASH_IDENTIFIER, db.buildExpectedAttributeValue(element.getHash(), true));

        if (!IDENTIFIER.equals("")) {
            expectationbuilder.put(IDENTIFIER, db.buildExpectedAttributeValue(element.getRange(), true));
        }

        DynamoDBDeleteExpression saveExpr = new DynamoDBDeleteExpression();
        saveExpr.setExpected(expectationbuilder.build());

        return saveExpr;
    }
}
