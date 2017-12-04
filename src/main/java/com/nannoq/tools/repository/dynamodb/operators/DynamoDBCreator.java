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
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.google.common.collect.ImmutableMap;
import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.DynamoDBModel;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.repository.cache.CacheManager;
import com.nannoq.tools.repository.repository.etag.ETagManager;
import com.nannoq.tools.repository.repository.redis.RedisUtils;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.serviceproxy.ServiceException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

/**
 * This class defines the creation operations for the DynamoDBRepository.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class DynamoDBCreator<E extends DynamoDBModel & Model & ETagable & Cacheable> {
    private static final Logger logger = LogManager.getLogger(DynamoDBCreator.class.getSimpleName());

    private final Class<E> TYPE;
    private final Vertx vertx;
    private final DynamoDBRepository<E> db;

    private final CacheManager<E> cacheManager;
    private final ETagManager<E> eTagManager;

    private final String HASH_IDENTIFIER;
    private final String IDENTIFIER;

    private final DynamoDBMapper DYNAMO_DB_MAPPER;
    private final RedisClient REDIS_CLIENT;

    private final Function<E, String> shortCacheIdSupplier;
    private final Function<E, String> cacheIdSupplier;

    public DynamoDBCreator(Class<E> type, Vertx vertx, DynamoDBRepository<E> db,
                           String HASH_IDENTIFIER, String IDENTIFIER,
                           CacheManager<E> cacheManager,
                           ETagManager<E> eTagManager) {
        TYPE = type;
        this.vertx = vertx;
        this.db = db;
        this.cacheManager = cacheManager;
        this.DYNAMO_DB_MAPPER = db.getDynamoDbMapper();
        this.REDIS_CLIENT = db.getRedisClient();
        this.HASH_IDENTIFIER = HASH_IDENTIFIER;
        this.IDENTIFIER = IDENTIFIER;
        this.eTagManager = eTagManager;
        cacheIdSupplier = e -> {
            String hash = e.getHash();
            String range = e.getRange();

            return TYPE.getSimpleName() + "_" + hash + (range == null || range.equals("") ? "" : "/" + range);
        };

        shortCacheIdSupplier = e -> {
            String hash = e.getHash();

            return TYPE.getSimpleName() + "_" + hash;
        };
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void doWrite(boolean create, Map<E, Function<E, E>> writeMap, Handler<AsyncResult<List<E>>> resultHandler) {
        vertx.<List<E>>executeBlocking(future -> {
            try {
                List<Future> writeFutures = new ArrayList<>();

                writeMap.forEach((E record, Function<E, E> updateLogic) -> {
                    Future<E> writeFuture = Future.future();

                    if (!create && updateLogic != null) {
                        if (logger.isDebugEnabled()) { logger.debug("Running remoteUpdate..."); }

                        try {
                            this.optimisticLockingSave(null, updateLogic, null, writeFuture, record);
                        } catch (Exception e) {
                            logger.error(e);

                            writeFuture.fail(e);
                        }

                        writeFutures.add(writeFuture);
                    } else {
                        if (logger.isDebugEnabled()) { logger.debug("Running remoteCreate..."); }

                        if (eTagManager != null) {
                            eTagManager.setSingleRecordEtag(record.generateAndSetEtag(new HashMap<>()), tagResult ->
                                    RedisUtils.performJedisWithRetry(REDIS_CLIENT, tagResult.result()));
                        }

                        try {
                            E finalRecord = db.setCreatedAt(db.setUpdatedAt(record));
                            final List<E> es = Collections.singletonList(finalRecord);

                            DYNAMO_DB_MAPPER.save(finalRecord, buildExistingExpression(finalRecord, false));
                            Future<Boolean> purgeFuture = Future.future();
                            destroyEtagsAfterCachePurge(writeFuture, finalRecord, purgeFuture, record.getHash());

                            cacheManager.replaceCache(purgeFuture, es, shortCacheIdSupplier, cacheIdSupplier);
                        } catch (Exception e) {
                            writeFuture.fail(e);
                        }

                        writeFutures.add(writeFuture);
                    }
                });

                CompositeFuture.all(writeFutures).setHandler(res -> {
                    if (res.failed()) {
                        future.fail(res.cause());
                    } else {
                        //noinspection unchecked
                        future.complete(writeFutures.stream()
                                .map(finalFuture -> (E) finalFuture.result())
                                .collect(toList()));
                    }
                });
            } catch (AmazonServiceException ase) {
                logger.error("Could not complete DynamoDB Operation, " +
                        "Error Message:  " + ase.getMessage() + ", " +
                        "HTTP Status:    " + ase.getStatusCode() + ", " +
                        "AWS Error Code: " + ase.getErrorCode() + ", " +
                        "Error Type:     " + ase.getErrorType() + ", " +
                        "Request ID:     " + ase.getRequestId(), ase);

                future.fail(ase);
            } catch (AmazonClientException ace) {
                logger.error("Internal Dynamodb Error, " + "Error Message:  " + ace.getMessage(), ace);

                future.fail(ace);
            } catch (Exception e) {
                logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()), e);

                future.fail(e);
            }
        }, false, result -> {
            if (result.failed()) {
                logger.error("Error in doWrite!", result.cause());

                resultHandler.handle(ServiceException.fail(500,
                        "An error occured when running doWrite: " + result.cause().getMessage(),
                        new JsonObject(Json.encode(result.cause()))));
            } else {
                resultHandler.handle(Future.succeededFuture(result.result()));
            }
        });
    }

    private void optimisticLockingSave(E newerVersion, Function<E, E> updateLogic,
                                       Integer prevCounter, Future<E> writeFuture, E record) {
        Integer counter = 0;
        if (prevCounter != null) counter = prevCounter;

        try {
            if (newerVersion != null) {
                newerVersion = updateLogic.apply(newerVersion);
                newerVersion = db.setUpdatedAt(newerVersion);

                if (eTagManager != null) {
                    eTagManager.setSingleRecordEtag(newerVersion.generateAndSetEtag(new HashMap<>()),
                            tagResult -> RedisUtils.performJedisWithRetry(
                                    REDIS_CLIENT, tagResult.result()));
                }

                if (logger.isDebugEnabled()) { logger.debug("Performing " + counter + " remoteUpdate!"); }
                DYNAMO_DB_MAPPER.save(newerVersion, buildExistingExpression(newerVersion, true));
                Future<Boolean> purgeFuture = Future.future();
                destroyEtagsAfterCachePurge(writeFuture, record, purgeFuture, record.getHash());

                cacheManager.replaceCache(purgeFuture, Collections.singletonList(newerVersion),
                        shortCacheIdSupplier, cacheIdSupplier);
                if (logger.isDebugEnabled()) { logger.debug("Update " + counter + " performed successfully!"); }
            } else {
                E updatedRecord = updateLogic.apply(record);
                newerVersion = db.setUpdatedAt(updatedRecord);

                if (eTagManager != null) {
                    eTagManager.setSingleRecordEtag(updatedRecord.generateAndSetEtag(new HashMap<>()),
                            tagResult -> RedisUtils.performJedisWithRetry(
                                    REDIS_CLIENT, tagResult.result()));
                }

                if (logger.isDebugEnabled()) { logger.debug("Performing immediate remoteUpdate!"); }
                DYNAMO_DB_MAPPER.save(updatedRecord, buildExistingExpression(record, true));
                Future<Boolean> purgeFuture = Future.future();
                purgeFuture.setHandler(purgeRes ->
                        destroyEtagsAfterCachePurge(writeFuture, record, purgeFuture, record.getHash()));

                cacheManager.replaceCache(purgeFuture, Collections.singletonList(updatedRecord),
                        shortCacheIdSupplier, cacheIdSupplier);
                if (logger.isDebugEnabled()) { logger.debug("Immediate remoteUpdate performed!"); }
            }
        } catch (ConditionalCheckFailedException e) {
            logger.error("SaveCollision on: " +
                    record.getClass().getSimpleName() + " : " + record.getHash() + " : " + record.getRange() + ", " +
                    "Error Message:  " + e.getMessage() + ", " +
                    "HTTP Status:    " + e.getStatusCode() + ", " +
                    "AWS Error Code: " + e.getErrorCode() + ", " +
                    "Error Type:     " + e.getErrorType() + ", " +
                    "Request ID:     " + e.getRequestId() + ", " +
                    ", retrying...");

            if (counter > 100) {
                logger.error(Json.encodePrettily(record) + "\n:\n" + Json.encodePrettily(newerVersion));

                throw new InternalError();
            }

            E newestRecord = db.fetchNewestRecord(TYPE, record.getHash(), record.getRange());

            optimisticLockingSave(newestRecord, updateLogic, ++counter, writeFuture, record);
        } catch (AmazonServiceException ase) {
            logger.error("Could not complete DynamoDB Operation, " +
                    "Error Message:  " + ase.getMessage() + ", " +
                    "HTTP Status:    " + ase.getStatusCode() + ", " +
                    "AWS Error Code: " + ase.getErrorCode() + ", " +
                    "Error Type:     " + ase.getErrorType() + ", " +
                    "Request ID:     " + ase.getRequestId());

            if (counter > 100) {
                logger.error(Json.encodePrettily(record) + "\n:\n" + Json.encodePrettily(newerVersion));

                throw new InternalError();
            }

            E newestRecord = db.fetchNewestRecord(TYPE, record.getHash(), record.getRange());

            optimisticLockingSave(newestRecord, updateLogic, ++counter, writeFuture, record);
        } catch (AmazonClientException ace) {
            logger.error("Internal Dynamodb Error, " + "Error Message:  " + ace.getMessage());

            if (counter > 100) {
                logger.error(Json.encodePrettily(record) + "\n:\n" + Json.encodePrettily(newerVersion));

                throw new InternalError();
            }

            E newestRecord = db.fetchNewestRecord(TYPE, record.getHash(), record.getRange());

            optimisticLockingSave(newestRecord, updateLogic, ++counter, writeFuture, record);
        }
    }

    private void destroyEtagsAfterCachePurge(Future<E> writeFuture, E record, Future<Boolean> purgeFuture, String hash) {
        purgeFuture.setHandler(purgeRes -> {
            if (purgeRes.failed()) {
                if (eTagManager != null) {
                    eTagManager.destroyEtags(record.getHash(), res ->
                            writeFuture.complete(record));
                } else {
                    writeFuture.complete(record);
                }
            } else {
                if (eTagManager != null) {
                    Future<Boolean> removeProjections = Future.future();
                    Future<Boolean> removeETags = Future.future();

                    eTagManager.removeProjectionsEtags(hash, removeProjections.completer());
                    eTagManager.destroyEtags(record.getHash(), removeETags.completer());

                    CompositeFuture.all(removeProjections, removeETags).setHandler(res -> {
                        if (res.failed()) {
                            writeFuture.fail(res.cause());
                        } else {
                            writeFuture.complete(record);
                        }
                    });
                } else {
                    writeFuture.complete(record);
                }
            }
        });
    }

    private DynamoDBSaveExpression buildExistingExpression(E element, boolean exists) {
        ImmutableMap.Builder<String, ExpectedAttributeValue> expectationbuilder =
                new ImmutableMap.Builder<String, ExpectedAttributeValue>()
                        .put(HASH_IDENTIFIER, db.buildExpectedAttributeValue(element.getHash(), exists));

        if (!IDENTIFIER.equals("")) {
            expectationbuilder.put(IDENTIFIER, db.buildExpectedAttributeValue(element.getRange(), exists));
        }

        DynamoDBSaveExpression saveExpr = new DynamoDBSaveExpression();
        saveExpr.setExpected(expectationbuilder.build());

        return saveExpr;
    }
}
