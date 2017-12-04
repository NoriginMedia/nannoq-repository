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

package com.nannoq.tools.repository.dynamodb.operators;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDeleteExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.KeyPair;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.google.common.collect.ImmutableMap;
import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.DynamoDBModel;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.repository.cache.ClusterCacheManagerImpl;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.serviceproxy.ServiceException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * This class defines the deletion operations for the DynamoDBRepository.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class DynamoDBDeleter<E extends DynamoDBModel & Model & ETagable & Cacheable> {
    private static final Logger logger = LogManager.getLogger(DynamoDBDeleter.class.getSimpleName());

    private final Class<E> TYPE;
    private final Vertx vertx;
    private final DynamoDBRepository<E> db;
    private final ClusterCacheManagerImpl<E> clusterCacheManagerImpl;

    private final String HASH_IDENTIFIER;
    private final String IDENTIFIER;

    private final DynamoDBMapper DYNAMO_DB_MAPPER;

    public DynamoDBDeleter(Class<E> type, Vertx vertx, DynamoDBRepository<E> db,
                           String HASH_IDENTIFIER, String IDENTIFIER,
                           ClusterCacheManagerImpl<E> clusterCacheManagerImpl) {
        TYPE = type;
        this.vertx = vertx;
        this.db = db;
        this.clusterCacheManagerImpl = clusterCacheManagerImpl;
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

                        clusterCacheManagerImpl.purgeCache(purgeFuture, items, e -> {
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
