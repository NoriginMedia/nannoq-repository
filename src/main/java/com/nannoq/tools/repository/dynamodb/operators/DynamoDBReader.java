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
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.DynamoDBModel;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.repository.cache.CacheManager;
import com.nannoq.tools.repository.repository.etag.ETagManager;
import com.nannoq.tools.repository.repository.results.ItemListResult;
import com.nannoq.tools.repository.repository.results.ItemResult;
import com.nannoq.tools.repository.utils.FilterParameter;
import com.nannoq.tools.repository.utils.ItemList;
import com.nannoq.tools.repository.utils.OrderByParameter;
import com.nannoq.tools.repository.utils.QueryPack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ServiceException;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.nannoq.tools.repository.dynamodb.DynamoDBRepository.PAGINATION_INDEX;
import static com.nannoq.tools.repository.repository.Repository.MULTIPLE_KEY;
import static java.util.stream.Collectors.toList;

/**
 * This class defines the read operations for the DynamoDBRepository.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class DynamoDBReader<E extends DynamoDBModel & Model & ETagable & Cacheable> {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBReader.class.getSimpleName());

    private final Class<E> TYPE;
    private final Vertx vertx;
    private final DynamoDBRepository<E> db;
    private final DynamoDBMapper DYNAMO_DB_MAPPER;

    private final String COLLECTION;
    private final String HASH_IDENTIFIER;
    private final String IDENTIFIER;
    private final String PAGINATION_IDENTIFIER;

    private final Map<String, JsonObject> GSI_KEY_MAP;

    private final DynamoDBParameters<E> dbParams;
    private final CacheManager<E> cacheManager;
    private final ETagManager<E> etagManager;

    private final int coreNum = Runtime.getRuntime().availableProcessors() * 2;

    public DynamoDBReader(Class<E> type, Vertx vertx, DynamoDBRepository<E> db, String COLLECTION,
                          String HASH_IDENTIFIER, String IDENTIFIER, String PAGINATION_IDENTIFIER,
                          Map<String, JsonObject> GSI_KEY_MAP,
                          DynamoDBParameters<E> dbParams, CacheManager<E> cacheManager, ETagManager<E> etagManager) {
        TYPE = type;
        this.vertx = vertx;
        this.db = db;
        this.COLLECTION = COLLECTION;
        this.HASH_IDENTIFIER = HASH_IDENTIFIER;
        this.IDENTIFIER = IDENTIFIER;
        this.PAGINATION_IDENTIFIER = PAGINATION_IDENTIFIER;
        this.DYNAMO_DB_MAPPER = db.getDynamoDbMapper();
        this.GSI_KEY_MAP = GSI_KEY_MAP;
        this.dbParams = dbParams;
        this.cacheManager = cacheManager;
        this.etagManager = etagManager;
    }

    public void read(JsonObject identifiers, Handler<AsyncResult<ItemResult<E>>> resultHandler) {
        AtomicLong startTime = new AtomicLong();
        startTime.set(System.nanoTime());
        AtomicLong preOperationTime = new AtomicLong();
        AtomicLong operationTime = new AtomicLong();
        AtomicLong postOperationTime = new AtomicLong();

        String hash = identifiers.getString("hash");
        String range = identifiers.getString("range");
        String cacheBase = TYPE.getSimpleName() + "_" + hash + (range == null ?
                (db.hasRangeKey() ? "/null" : "") : "/" + range);
        String cacheId = "FULL_CACHE_" + cacheBase;

        vertx.<E>executeBlocking(future -> cacheManager.checkObjectCache(cacheId, result -> {
            if (result.failed()) {
                future.fail(result.cause());
            } else {
                future.complete(result.result());
            }
        }), false, checkResult -> {
            if (checkResult.succeeded()) {
                resultHandler.handle(Future.succeededFuture(new ItemResult<>(checkResult.result(), true)));

                if (logger.isDebugEnabled()) { logger.debug("Served cached version of: " + cacheId); }
            } else {
                vertx.<E>executeBlocking(future -> {
                    E item = fetchItem(startTime, preOperationTime, operationTime, hash, range, true);

                    if (item != null && cacheManager.isObjectCacheAvailable()) {
                        cacheManager.replaceObjectCache(cacheBase, item, future, new String[]{});
                    } else {
                        if (item == null) {
                            future.fail(new NoSuchElementException());
                        } else {
                            future.complete(item);
                        }
                    }
                }, false, readResult -> {
                    if (readResult.failed()) {
                        doReadResult(postOperationTime, startTime, readResult, resultHandler);
                    } else {
                        postOperationTime.set(System.nanoTime() - startTime.get());

                        returnTimedResult(readResult, preOperationTime, operationTime, postOperationTime, resultHandler);
                    }
                });
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void read(JsonObject identifiers, boolean consistent, String[] projections,
                     Handler<AsyncResult<ItemResult<E>>> resultHandler) {
        AtomicLong startTime = new AtomicLong();
        startTime.set(System.nanoTime());
        AtomicLong preOperationTime = new AtomicLong();
        AtomicLong operationTime = new AtomicLong();
        AtomicLong postOperationTime = new AtomicLong();

        String hash = identifiers.getString("hash");
        String range = identifiers.getString("range");
        String cacheId = TYPE.getSimpleName() + "_" + hash + (range == null ? "" : "/" + range) +
                ((projections != null && projections.length > 0) ? "/projection/" + Arrays.hashCode(projections) : "");

        vertx.<E>executeBlocking(future -> cacheManager.checkObjectCache(cacheId, result -> {
            if (result.failed()) {
                future.fail(result.cause());
            } else {
                future.complete(result.result());
            }
        }), false, checkResult -> {
            if (checkResult.succeeded()) {
                resultHandler.handle(Future.succeededFuture(new ItemResult<>(checkResult.result(), true)));

                if (logger.isDebugEnabled()) { logger.debug("Served cached version of: " + cacheId); }
            } else {
                vertx.<E>executeBlocking(future -> {
                    E item = fetchItem(startTime, preOperationTime, operationTime, hash, range, consistent);

                    if (item != null) {
                        item.generateAndSetEtag(new ConcurrentHashMap<>());
                    }

                    if (etagManager != null) {
                        etagManager.setProjectionEtags(projections, identifiers.encode().hashCode(), item);
                    }

                    if (item != null && cacheManager.isObjectCacheAvailable()) {
                        cacheManager.replaceObjectCache(cacheId, item, future, projections == null ? new String[]{} : projections);
                    } else {
                        if (item == null) {
                            future.fail(new NoSuchElementException());
                        } else {
                            future.complete(item);
                        }
                    }
                }, false, readResult -> {
                    if (readResult.failed()) {
                        doReadResult(postOperationTime, startTime, readResult, resultHandler);
                    } else {
                        postOperationTime.set(System.nanoTime() - operationTime.get());

                        returnTimedResult(readResult, preOperationTime, operationTime, postOperationTime, resultHandler);
                    }
                });
            }
        });
    }

    private void doReadResult(AtomicLong postOperationTime, AtomicLong startTime, AsyncResult<E> readResult,
                              Handler<AsyncResult<ItemResult<E>>> resultHandler) {
        if (readResult.cause().getClass() == NoSuchElementException.class) {
            postOperationTime.set(System.nanoTime() - startTime.get());
            resultHandler.handle(ServiceException.fail(404, "Not found!",
                    new JsonObject(Json.encode(readResult.cause()))));
        } else {
            logger.error("Error in read!", readResult.cause());

            postOperationTime.set(System.nanoTime() - startTime.get());
            resultHandler.handle(ServiceException.fail(500, "Error in read!",
                    new JsonObject(Json.encode(readResult.cause()))));
        }
    }

    private E fetchItem(AtomicLong startTime, AtomicLong preOperationTime, AtomicLong operationTime,
                        String hash, String range, boolean consistent) {
        try {
            if (db.hasRangeKey()) {
                if (range == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Loading ranged item without range key!");
                    }

                    return fetchHashItem(hash, startTime, preOperationTime, operationTime, consistent);
                } else {
                    return fetchHashAndRangeItem(hash, range, startTime, preOperationTime, operationTime);
                }
            } else {
                return fetchHashItem(hash, startTime, preOperationTime, operationTime, consistent);
            }
        } catch (Exception e) {
            logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));
        }

        return null;
    }

    private E fetchHashAndRangeItem(String hash, String range,
                                    AtomicLong startTime, AtomicLong preOperationTime, AtomicLong operationTime) {
        long timeBefore = System.currentTimeMillis();

        preOperationTime.set(System.nanoTime() - startTime.get());
        E item = DYNAMO_DB_MAPPER.load(TYPE, hash, range);
        operationTime.set(System.nanoTime() - startTime.get());

        if (logger.isDebugEnabled()) {
            logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
        }

        return item;
    }

    private E fetchHashItem(String hash, AtomicLong startTime, AtomicLong preOperationTime, AtomicLong operationTime,
                            boolean consistent)
            throws IllegalAccessException, InstantiationException {
        DynamoDBQueryExpression<E> query =
                new DynamoDBQueryExpression<>();
        E keyObject = TYPE.newInstance();
        keyObject.setHash(hash);
        query.setConsistentRead(consistent);
        query.setHashKeyValues(keyObject);
        query.setLimit(1);

        long timeBefore = System.currentTimeMillis();

        preOperationTime.set(System.nanoTime() - startTime.get());
        List<E> items = DYNAMO_DB_MAPPER.query(TYPE, query);
        operationTime.set(System.nanoTime() - startTime.get());

        if (logger.isDebugEnabled()) {
            logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
        }

        if (!items.isEmpty()) return items.get(0);

        return null;
    }

    private void returnTimedResult(AsyncResult<E> readResult,
                                   AtomicLong preOperationTime, AtomicLong operationTime, AtomicLong postOperationTime,
                                   Handler<AsyncResult<ItemResult<E>>> resultHandler) {
        final ItemResult<E> eItemResult = new ItemResult<>(readResult.result(), false);
        eItemResult.setPreOperationProcessingTime(preOperationTime.get());
        eItemResult.setOperationProcessingTime(operationTime.get());
        eItemResult.setPostOperationProcessingTime(postOperationTime.get());

        resultHandler.handle(Future.succeededFuture(eItemResult));
    }

    @SuppressWarnings("unchecked")
    public void readAll(Handler<AsyncResult<List<E>>> resultHandler) {
        vertx.<List<E>>executeBlocking(future -> {
            try {
                long timeBefore = System.currentTimeMillis();

                PaginatedParallelScanList<E> items =
                        DYNAMO_DB_MAPPER.parallelScan(TYPE, new DynamoDBScanExpression(), coreNum);
                items.loadAllResults();

                if (logger.isDebugEnabled()) {
                    logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                }

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
                logger.error("Error in readAll!", readResult.cause());

                resultHandler.handle(ServiceException.fail(500, "Error in readAll!",
                        new JsonObject(Json.encode(readResult.cause()))));
            } else {
                resultHandler.handle(Future.succeededFuture(readResult.result()));
            }
        });
    }

    public void readAllPaginated(Handler<AsyncResult<PaginatedParallelScanList<E>>> resultHandler) {
        vertx.<PaginatedParallelScanList<E>>executeBlocking(future -> {
            try {
                long timeBefore = System.currentTimeMillis();

                PaginatedParallelScanList<E> items =
                        DYNAMO_DB_MAPPER.parallelScan(TYPE, new DynamoDBScanExpression(), coreNum);

                if (logger.isDebugEnabled()) {
                    logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                }

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
                logger.error("Error in readAllPaginated!", readResult.cause());

                resultHandler.handle(ServiceException.fail(500, "Error in readAll!",
                        new JsonObject(Json.encode(readResult.cause()))));
            } else {
                resultHandler.handle(Future.succeededFuture(readResult.result()));
            }
        });
    }

    public void readAll(JsonObject identifiers, Map<String, List<FilterParameter>> filterParameterMap,
                        Handler<AsyncResult<List<E>>> resultHandler) {
        vertx.<List<E>>executeBlocking(future -> {
            try {
                String identifier = identifiers.getString("hash");
                DynamoDBQueryExpression<E> filterExpression = dbParams.applyParameters(null, filterParameterMap);

                if (filterExpression.getKeyConditionExpression() == null) {
                    E keyItem = TYPE.newInstance();
                    keyItem.setHash(identifier);
                    filterExpression.setHashKeyValues(keyItem);
                }

                filterExpression.setConsistentRead(false);

                long timeBefore = System.currentTimeMillis();

                PaginatedQueryList<E> items = DYNAMO_DB_MAPPER.query(TYPE, filterExpression);
                items.loadAllResults();

                if (logger.isDebugEnabled()) {
                    logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                }

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
                logger.error("Error in Read All!", readResult.cause());

                resultHandler.handle(ServiceException.fail(500, "Error in readAllWithoutPagination!",
                        new JsonObject(Json.encode(readResult.cause()))));
            } else {
                resultHandler.handle(Future.succeededFuture(readResult.result()));
            }
        });
    }

    public void readAll(JsonObject identifiers, String pageToken, QueryPack queryPack, String[] projections,
                        Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        readAll(identifiers, pageToken, queryPack, projections, null, resultHandler);
    }

    public void readAll(String pageToken, QueryPack queryPack, String[] projections,
                        Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        readAll(new JsonObject(), pageToken, queryPack, projections, null, resultHandler);
    }

    public void readAll(JsonObject identifiers, String pageToken, QueryPack queryPack, String[] projections,
                        String GSI, Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        AtomicLong startTime = new AtomicLong();
        startTime.set(System.nanoTime());

        String hash = identifiers.getString("hash");
        String cacheId = TYPE.getSimpleName() + "_" + hash +
                (queryPack.getBaseEtagKey() != null ? "/" + queryPack.getBaseEtagKey() : "/START");

        if (logger.isDebugEnabled()) { logger.debug("Running readAll with: " + hash + " : " + cacheId); }

        vertx.<ItemList<E>>executeBlocking(future -> cacheManager.checkItemListCache(cacheId, projections, result -> {
            if (result.failed()) {
                future.fail(result.cause());
            } else {
                future.complete(result.result());
            }
        }), false, checkResult -> {
            if (checkResult.succeeded()) {
                resultHandler.handle(Future.succeededFuture(new ItemListResult<>(checkResult.result(), true)));

                if (logger.isDebugEnabled()) { logger.debug("Served cached version of: " + cacheId); }
            } else {
                Queue<OrderByParameter> orderByQueue = queryPack.getOrderByQueue();
                Map<String, List<FilterParameter>> params = queryPack.getParams();
                String indexName = queryPack.getIndexName();
                Integer limit = queryPack.getLimit();

                if (logger.isDebugEnabled()) {
                    logger.debug("Building expression with: " + Json.encodePrettily(queryPack));
                }

                DynamoDBQueryExpression<E> filterExpression = null;
                Boolean multiple = identifiers.getBoolean(MULTIPLE_KEY);

                if ((multiple == null || !multiple) && (orderByQueue != null || params != null)) {
                    filterExpression = dbParams.applyParameters(orderByQueue != null ? orderByQueue.peek() : null,
                            params);
                    filterExpression = dbParams.applyOrderBy(orderByQueue, GSI, indexName, filterExpression);
                    filterExpression.setLimit((limit == null || limit == 0) ? 20 : limit);

                    if (logger.isDebugEnabled()) { logger.debug("Custom filter is: " +
                            "\nIndex: " + filterExpression.getIndexName() +
                            "\nLimit: " + filterExpression.getLimit() +
                            " (" + (limit == null ? 20 : limit) + ") " +
                            "\nExpression: " + filterExpression.getFilterExpression() +
                            "\nRange Key Condition: " + filterExpression.getRangeKeyConditions() +
                            "\nAsc: " + filterExpression.isScanIndexForward()); }
                }

                String etagKey = queryPack.getBaseEtagKey();

                returnDatabaseContent(queryPack, identifiers, pageToken, hash, etagKey, cacheId,
                        filterExpression, projections, GSI, startTime, resultHandler);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void returnDatabaseContent(QueryPack queryPack, JsonObject identifiers, String pageToken,
                                       String hash, String etagKey,
                                       String cacheId, DynamoDBQueryExpression<E> filteringExpression,
                                       String[] projections, String GSI,
                                       AtomicLong startTime, Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        vertx.<ItemListResult<E>>executeBlocking(future -> {
            Boolean multiple = identifiers.getBoolean(MULTIPLE_KEY);
            boolean unFilteredIndex = filteringExpression == null;
            String alternateIndex = null;
            Map<String, List<FilterParameter>> params = queryPack.getParams();
            List<FilterParameter> nameParams = params == null ? null : params.get(PAGINATION_IDENTIFIER);
            Future<ItemListResult<E>> itemListFuture = Future.future();

            if (logger.isDebugEnabled()) {
                logger.debug("Do remoteRead");
            }

            try {
                if (filteringExpression != null) {
                    alternateIndex = db.getAlternativeIndexIdentifier(filteringExpression.getIndexName());
                }

                if (identifiers.isEmpty() || identifiers.getString("hash") == null) {
                    runRootQuery(queryPack.getBaseEtagKey(), multiple, identifiers, hash, queryPack, filteringExpression,
                            GSI, projections, pageToken, unFilteredIndex, alternateIndex, startTime,
                            itemListFuture.completer());
                } else if (params != null && nameParams != null && dbParams.isIllegalRangedKeyQueryParams(nameParams)) {
                    runIllegalRangedKeyQueryAsScan(queryPack.getBaseEtagKey(), hash, queryPack,
                            GSI, projections, pageToken, unFilteredIndex, alternateIndex, startTime,
                            itemListFuture.completer());
                } else {
                    runStandardQuery(queryPack.getBaseEtagKey(), multiple, identifiers, hash, filteringExpression,
                            GSI, projections, pageToken, unFilteredIndex, alternateIndex, startTime,
                            itemListFuture.completer());
                }

                itemListFuture.setHandler(itemListResult -> {
                    if (itemListResult.failed()) {
                        future.fail(itemListResult.cause());
                    } else {
                        ItemListResult<E> returnList = itemListResult.result();
                        ItemList<E> itemList = returnList.getItemList();

                        if (logger.isDebugEnabled()) {
                            logger.debug("Constructed items!");
                        }
                        Future<Boolean> itemListCacheFuture = Future.future();

                        if (cacheManager.isItemListCacheAvailable()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Constructing cache!");
                            }

                            JsonObject cacheObject = itemList.toJson(projections);

                            if (logger.isDebugEnabled()) {
                                logger.debug("Cache complete!");
                            }

                            String content = cacheObject.encode();

                            if (logger.isDebugEnabled()) {
                                logger.debug("Cache encoded!");
                            }

                            cacheManager.replaceItemListCache(content, () -> cacheId, cacheRes -> {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Setting: " + etagKey + " with: " + itemList.getEtag());
                                }

                                String etagItemListHashKey = TYPE.getSimpleName() + "_" +
                                        identifiers.encode().hashCode() + "_" + "itemListEtags";

                                if (etagManager != null) {
                                    etagManager.setItemListEtags(etagItemListHashKey, etagKey, itemList, itemListCacheFuture);
                                } else {
                                    itemListCacheFuture.complete();
                                }
                            });
                        } else {
                            itemListCacheFuture.complete();
                        }

                        itemListCacheFuture.setHandler(res -> future.complete(returnList));
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
        }, false, readAllResult -> {
            if (readAllResult.failed()) {
                resultHandler.handle(ServiceException.fail(500, "Error in readAll!",
                        new JsonObject(Json.encode(readAllResult.cause()))));
            } else {
                resultHandler.handle(Future.succeededFuture(readAllResult.result()));
            }
        });
    }

    private void runStandardQuery(String baseEtagKey, Boolean multiple, JsonObject identifiers, String hash,
                                  DynamoDBQueryExpression<E> filteringExpression, String GSI, String[] projections,
                                  String pageToken, boolean unFilteredIndex, String alternateIndex,
                                  AtomicLong startTime, Handler<AsyncResult<ItemListResult<E>>> resultHandler)
            throws InstantiationException, IllegalAccessException {
        if (multiple != null && multiple) {
            standardMultipleQuery(baseEtagKey, identifiers, hash, filteringExpression, pageToken, GSI, unFilteredIndex,
                    alternateIndex, projections, startTime, resultHandler);
        } else {
            standardQuery(baseEtagKey, identifiers, hash, filteringExpression, pageToken, GSI, unFilteredIndex,
                    alternateIndex, projections, startTime, resultHandler);
        }
    }

    private void standardMultipleQuery(String baseEtagKey, JsonObject identifiers, String hash,
                                       DynamoDBQueryExpression<E> filteringExpression,
                                       String pageToken, String GSI, boolean unFilteredIndex, String alternateIndex,
                                       String[] projections, AtomicLong startTime,
                                       Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        AtomicLong preOperationTime = new AtomicLong();
        AtomicLong operationTime = new AtomicLong();
        AtomicLong postOperationTime = new AtomicLong();

        if (logger.isDebugEnabled()) {
            logger.debug("Running multiple id query...");
        }

        List<String> multipleIds = identifiers.getJsonArray("range").stream()
                .map(Object::toString)
                .collect(toList());

        List<KeyPair> keyPairsList = multipleIds.stream()
                .distinct()
                .map(id -> {
                    if (hashOnlyModel()) {
                        return new KeyPair().withHashKey(id);
                    } else {
                        return new KeyPair().withHashKey(hash).withRangeKey(id);
                    }
                }).collect(toList());

        Map<Class<?>, List<KeyPair>> keyPairs = new HashMap<>();
        keyPairs.put(TYPE, keyPairsList);

        if (logger.isDebugEnabled()) {
            logger.debug("Keypairs: " + Json.encodePrettily(keyPairs));
        }

        long timeBefore = System.currentTimeMillis();

        preOperationTime.set(System.nanoTime() - startTime.get());
        Map<String, List<Object>> items = DYNAMO_DB_MAPPER.batchLoad(keyPairs);
        operationTime.set(System.nanoTime() - preOperationTime.get());

        int pageCount = items.get(COLLECTION).size();
        int desiredCount = filteringExpression != null && filteringExpression.getLimit() != null ?
                filteringExpression.getLimit() : 20;

        if (logger.isDebugEnabled()) {
            logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Items Returned for collection: " + pageCount);
        }

        @SuppressWarnings("unchecked")
        List<E> allItems = items.get(COLLECTION).stream()
                .map(item -> (E) item)
                .sorted(Comparator.comparing(DynamoDBModel::getRange,
                        Comparator.comparingInt(multipleIds::indexOf)))
                .collect(toList());

        if (pageToken != null) {
            final Map<String, AttributeValue> pageTokenMap =
                    getTokenMap(pageToken, GSI, PAGINATION_IDENTIFIER);
            String id = pageTokenMap.get(IDENTIFIER).getS();

            final Optional<E> first = allItems.stream()
                    .filter(item -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Id is: " + id + ", Range is: " + item.getRange());
                        }

                        return item.getRange().equals(id);
                    })
                    .findFirst();

            if (first.isPresent()) {
                allItems = allItems.subList(allItems.indexOf(first.get()) + 1, allItems.size());
            }
        }

        List<E> itemList = allItems.stream()
                .limit(desiredCount)
                .collect(toList());

        pageCount = allItems.size();
        int count = pageCount < desiredCount ? pageCount : desiredCount;
        String pagingToken = setScanPageToken(pageCount, desiredCount, itemList, GSI, alternateIndex, unFilteredIndex);

        returnTimedItemListResult(baseEtagKey, resultHandler, count, pagingToken, itemList, projections,
                preOperationTime, operationTime, postOperationTime);
    }

    private void returnTimedItemListResult(String baseEtagKey, Handler<AsyncResult<ItemListResult<E>>> resultHandler, int count,
                                           String pagingToken, List<E> itemList, String[] projections,
                                           AtomicLong preOperationTime, AtomicLong operationTime,
                                           AtomicLong postOperationTime) {
        postOperationTime.set(System.nanoTime() - operationTime.get());
        final ItemListResult<E> eItemListResult =
                new ItemListResult<>(baseEtagKey, count, itemList, pagingToken, projections, false);
        eItemListResult.setPreOperationProcessingTime(preOperationTime.get());
        eItemListResult.setOperationProcessingTime(operationTime.get());
        eItemListResult.setPostOperationProcessingTime(postOperationTime.get());

        resultHandler.handle(Future.succeededFuture(eItemListResult));
    }

    private void standardQuery(String baseEtagKey, JsonObject identifiers, String hash, DynamoDBQueryExpression<E> filteringExpression,
                               String pageToken, String GSI, boolean unFilteredIndex, String alternateIndex,
                               String[] projections, AtomicLong startTime, Handler<AsyncResult<ItemListResult<E>>> resultHandler)
            throws IllegalAccessException, InstantiationException {
        AtomicLong preOperationTime = new AtomicLong();
        AtomicLong operationTime = new AtomicLong();
        AtomicLong postOperationTime = new AtomicLong();
        DynamoDBQueryExpression<E> queryExpression;

        String[] effectivelyFinalProjections = dbParams.buildProjections(projections,
                (identifiers.isEmpty() || filteringExpression == null) ?
                        getPaginationIndex() : alternateIndex);

        if (logger.isDebugEnabled()) {
            logger.debug("Running standard query...");
        }

        if (filteringExpression == null) {
            queryExpression = new DynamoDBQueryExpression<>();
            queryExpression.setIndexName(GSI != null ? GSI : getPaginationIndex());
            queryExpression.setLimit(20);
            queryExpression.setScanIndexForward(false);
            queryExpression.setExclusiveStartKey(getTokenMap(pageToken, GSI, PAGINATION_IDENTIFIER));
        } else {
            queryExpression = filteringExpression;
            queryExpression.setExclusiveStartKey(getTokenMap(pageToken, GSI,
                    alternateIndex == null ? PAGINATION_IDENTIFIER : alternateIndex));
        }

        if (GSI == null) {
            if (queryExpression.getKeyConditionExpression() == null) {
                E keyItem = TYPE.newInstance();
                keyItem.setHash(hash);
                keyItem.setRange(null);
                queryExpression.setHashKeyValues(keyItem);
            }
        } else {
            if (queryExpression.getKeyConditionExpression() == null) {
                setFilterExpressionKeyCondition(queryExpression, GSI, hash);
            }
        }

        queryExpression.setConsistentRead(false);

        int desiredCount = 0;

        if (!unFilteredIndex) {
            desiredCount = queryExpression.getLimit();
            queryExpression.setLimit(null);
        }

        setProjectionsOnQueryExpression(queryExpression, effectivelyFinalProjections);

        if (logger.isDebugEnabled()) {
            logger.debug(Json.encodePrettily(queryExpression));
        }

        long timeBefore = System.currentTimeMillis();

        preOperationTime.set(System.nanoTime() - startTime.get());
        QueryResultPage<E> queryPageResults =
                DYNAMO_DB_MAPPER.queryPage(TYPE, queryExpression);
        operationTime.set(System.nanoTime() - preOperationTime.get());

        if (logger.isDebugEnabled()) {
            logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Scanned items: " + queryPageResults.getScannedCount());
        }

        int pageCount, count;
        List<E> itemList;
        String pagingToken;
        Map<String, AttributeValue> lastEvaluatedKey;

        if (!unFilteredIndex) {
            pageCount = queryPageResults.getCount();
            itemList = queryPageResults.getResults()
                    .subList(0, pageCount < desiredCount ? pageCount : desiredCount);
            count = pageCount < desiredCount ? pageCount : desiredCount;

            pagingToken = setScanPageToken(pageCount, desiredCount, itemList, GSI, alternateIndex, false);
        } else {
            count = queryPageResults.getCount();
            itemList = queryPageResults.getResults();
            lastEvaluatedKey = queryPageResults.getLastEvaluatedKey();
            pagingToken = lastEvaluatedKey == null ? "END_OF_LIST" :
                    setPageToken(lastEvaluatedKey, GSI, true, alternateIndex);

        }

        returnTimedItemListResult(baseEtagKey, resultHandler, count, pagingToken, itemList, projections,
                preOperationTime, operationTime, postOperationTime);
    }

    private void runIllegalRangedKeyQueryAsScan(String baseEtagKey, String hash, QueryPack queryPack,
                                                String GSI, String[] projections, String pageToken,
                                                boolean unFilteredIndex, String alternateIndex,
                                                AtomicLong startTime, Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        AtomicLong preOperationTime = new AtomicLong();
        AtomicLong operationTime = new AtomicLong();
        AtomicLong postOperationTime = new AtomicLong();

        if (logger.isDebugEnabled()) {
            logger.debug("Running illegal rangedKey query...");
        }

        String hashScanKey = "#HASH_KEY_VALUE";
        String hashScanValue = ":HASH_VALUE";

        DynamoDBScanExpression scanExpression = dbParams.applyParameters(queryPack.getParams());
        String conditionString = scanExpression.getFilterExpression();
        if (hash != null) conditionString += " AND " + hashScanKey + " = " + hashScanValue;
        scanExpression.setFilterExpression(conditionString);

        if (GSI == null) {
            scanExpression.getExpressionAttributeNames().put(hashScanKey, HASH_IDENTIFIER);
        } else {
            scanExpression.getExpressionAttributeNames().put(hashScanKey, GSI_KEY_MAP.get(GSI).getString("hash"));
        }

        scanExpression.getExpressionAttributeValues().put(hashScanValue, new AttributeValue().withS(hash));

        final Map<String, AttributeValue> pageTokenMap = getTokenMap(pageToken, GSI, PAGINATION_IDENTIFIER);

        scanExpression.setLimit(null);
        scanExpression.setIndexName(GSI != null ? GSI : getPaginationIndex());
        scanExpression.setConsistentRead(false);
        setProjectionsOnScanExpression(scanExpression, projections);

        long timeBefore = System.currentTimeMillis();

        if (logger.isDebugEnabled()) {
            logger.debug("Scan expression is: " + Json.encodePrettily(scanExpression));
        }

        preOperationTime.set(System.nanoTime() - startTime.get());
        ScanResultPage<E> items = DYNAMO_DB_MAPPER.scanPage(TYPE, scanExpression);
        operationTime.set(System.nanoTime() - preOperationTime.get());

        int pageCount = items.getCount();
        int desiredCount = scanExpression.getLimit() != null ? scanExpression.getLimit() : 20;

        if (logger.isDebugEnabled()) {
            logger.debug(pageCount + " results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
        }

        @SuppressWarnings("UnnecessaryLocalVariable")
        Queue<OrderByParameter> queue = queryPack.getOrderByQueue();

        final boolean orderIsAscending = queue != null && queue.size() > 0 && queue.peek().getDirection().equals("asc");
        String finalAlternateIndex = queue != null && queue.size() > 0 ? queue.peek().getField() : PAGINATION_IDENTIFIER;
        final Comparator<E> comparator = Comparator.comparing(e -> db.getFieldAsString(finalAlternateIndex, e));
        final Comparator<E> comparing = orderIsAscending ? comparator : comparator.reversed();

        List<E> allItems = items.getResults().stream()
                .sorted(comparing)
                .collect(toList());

        if (pageToken != null) {
            String id = pageTokenMap.get(IDENTIFIER).getS();

            final Optional<E> first = allItems.stream()
                    .filter(item -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Id is: " + id + ", Range is: " + item.getRange());
                        }

                        return item.getRange().equals(id);
                    })
                    .findFirst();

            if (first.isPresent()) {
                allItems = allItems.subList(allItems.indexOf(first.get()) + 1, allItems.size());
            }
        }

        List<E> itemList = allItems.stream()
                .limit(desiredCount)
                .collect(toList());

        pageCount = allItems.size();
        int count = pageCount < desiredCount ? pageCount : desiredCount;
        String pagingToken = setScanPageToken(pageCount, desiredCount, itemList, GSI, alternateIndex, unFilteredIndex);

        returnTimedItemListResult(baseEtagKey, resultHandler, count, pagingToken, itemList, projections,
                preOperationTime, operationTime, postOperationTime);
    }

    private void runRootQuery(String baseEtagKey, Boolean multiple, JsonObject identifiers, String hash,
                              QueryPack queryPack, DynamoDBQueryExpression<E> filteringExpression,
                              String GSI, String[] projections, String pageToken, boolean unFilteredIndex,
                              String alternateIndex, AtomicLong startTime,
                              Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        if (multiple != null && multiple) {
            rootMultipleQuery(baseEtagKey, identifiers, hash, filteringExpression, GSI, pageToken, projections,
                    unFilteredIndex, alternateIndex, startTime, resultHandler);
        } else {
            rootRootQuery(baseEtagKey, queryPack, GSI, pageToken, projections, unFilteredIndex, alternateIndex, startTime, resultHandler);
        }
    }

    private void rootMultipleQuery(String baseEtagKey, JsonObject identifiers, String hash, DynamoDBQueryExpression<E> filteringExpression,
                                   String GSI, String pageToken, String[] projections, boolean unFilteredIndex,
                                   String alternateIndex, AtomicLong startTime,
                                   Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        AtomicLong preOperationTime = new AtomicLong();
        AtomicLong operationTime = new AtomicLong();
        AtomicLong postOperationTime = new AtomicLong();

        if (logger.isDebugEnabled()) {
            logger.debug("Running root multiple id query...");
        }

        List<String> multipleIds = identifiers.getJsonArray("range").stream()
                .map(Object::toString)
                .collect(toList());

        List<KeyPair> keyPairsList = multipleIds.stream()
                .distinct()
                .map(id -> {
                    if (hashOnlyModel()) {
                        return new KeyPair().withHashKey(id);
                    } else {
                        return new KeyPair().withHashKey(hash).withRangeKey(id);
                    }
                }).collect(toList());

        Map<Class<?>, List<KeyPair>> keyPairs = new HashMap<>();
        keyPairs.put(TYPE, keyPairsList);

        if (logger.isDebugEnabled()) {
            logger.debug("Keypairs: " + Json.encodePrettily(keyPairs));
        }

        long timeBefore = System.currentTimeMillis();

        preOperationTime.set(System.nanoTime() - startTime.get());
        Map<String, List<Object>> items = DYNAMO_DB_MAPPER.batchLoad(keyPairs);
        operationTime.set(System.nanoTime() - preOperationTime.get());

        int pageCount = items.get(COLLECTION).size();
        int desiredCount = filteringExpression != null && filteringExpression.getLimit() != null ?
                filteringExpression.getLimit() : 20;

        if (logger.isDebugEnabled()) {
            logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Items Returned for collection: " + pageCount);
        }

        @SuppressWarnings("unchecked")
        List<E> allItems = items.get(COLLECTION).stream()
                .map(item -> (E) item)
                .sorted(Comparator.comparing(hashOnlyModel() ? DynamoDBModel::getHash : DynamoDBModel::getRange,
                        Comparator.comparingInt(multipleIds::indexOf)))
                .collect(toList());

        if (pageToken != null) {
            final Map<String, AttributeValue> pageTokenMap =
                    getTokenMap(pageToken, GSI, PAGINATION_IDENTIFIER);
            String id = pageTokenMap.get(hashOnlyModel() ? HASH_IDENTIFIER : IDENTIFIER).getS();

            allItems = reduceByPageToken(allItems, id);
        }

        List<E> itemList = allItems.stream()
                .limit(desiredCount)
                .collect(toList());

        pageCount = allItems.size();
        int count = pageCount < desiredCount ? pageCount : desiredCount;
        String pagingToken = setScanPageToken(pageCount, desiredCount, itemList, GSI, alternateIndex, unFilteredIndex);

        returnTimedItemListResult(baseEtagKey, resultHandler, count, pagingToken, itemList, projections,
                preOperationTime, operationTime, postOperationTime);
    }

    private void rootRootQuery(String baseEtagKey, QueryPack queryPack, String GSI, String pageToken, String[] projections,
                               boolean unFilteredIndex, String alternateIndex, AtomicLong startTime,
                               Handler<AsyncResult<ItemListResult<E>>> resultHandler) {
        AtomicLong preOperationTime = new AtomicLong();
        AtomicLong operationTime = new AtomicLong();
        AtomicLong postOperationTime = new AtomicLong();

        if (logger.isDebugEnabled()) {
            logger.debug("Running root query...");
        }

        DynamoDBScanExpression scanExpression = dbParams.applyParameters(queryPack.getParams());
        final Map<String, AttributeValue> pageTokenMap = getTokenMap(pageToken, GSI, PAGINATION_IDENTIFIER);

        scanExpression.setLimit(null);
        scanExpression.setIndexName(GSI != null ? GSI : getPaginationIndex());
        scanExpression.setConsistentRead(false);
        setProjectionsOnScanExpression(scanExpression, projections);

        long timeBefore = System.currentTimeMillis();

        if (logger.isDebugEnabled()) {
            logger.debug("Scan expression is: " + Json.encodePrettily(scanExpression));
        }

        preOperationTime.set(System.nanoTime() - startTime.get());
        ScanResultPage<E> items =
                DYNAMO_DB_MAPPER.scanPage(TYPE, scanExpression);
        operationTime.set(System.nanoTime() - preOperationTime.get());

        int pageCount = items.getCount();
        int desiredCount = (queryPack.getLimit() == null || queryPack.getLimit() == 0) ? 20 : queryPack.getLimit();

        if (logger.isDebugEnabled()) {
            logger.debug("DesiredCount is: " + desiredCount);
            logger.debug(pageCount + " results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
        }

        @SuppressWarnings("UnnecessaryLocalVariable")
        Queue<OrderByParameter> queue = queryPack.getOrderByQueue();

        final boolean orderIsAscending =
                queue != null && queue.size() > 0 && queue.peek().getDirection().equals("asc");

        String finalAlternateIndex = queue != null && queue.size() > 0 ?
                queue.peek().getField() : PAGINATION_IDENTIFIER;

        final Comparator<E> comparator = Comparator.comparing(e -> db.getFieldAsString(finalAlternateIndex, e));
        final Comparator<E> comparing = orderIsAscending ? comparator : comparator.reversed();

        List<E> allItems = items.getResults().stream()
                .sorted(comparing)
                .collect(toList());

        if (pageToken != null) {
            String id = pageTokenMap.get(hashOnlyModel() ? HASH_IDENTIFIER : IDENTIFIER).getS();

            allItems = reduceByPageToken(allItems, id);
        }

        List<E> itemList = allItems.stream()
                .limit(desiredCount)
                .collect(toList());

        pageCount = allItems.size();
        int count = pageCount < desiredCount ? pageCount : desiredCount;
        String pagingToken = setScanPageToken(pageCount, desiredCount, itemList, GSI, alternateIndex, unFilteredIndex);

        returnTimedItemListResult(baseEtagKey, resultHandler, count, pagingToken, itemList, projections,
                preOperationTime, operationTime, postOperationTime);
    }

    @SuppressWarnings("Duplicates")
    private void setProjectionsOnScanExpression(DynamoDBScanExpression scanExpression, String[] projections) {
        if (projections != null && projections.length > 0) {
            scanExpression.withSelect("SPECIFIC_ATTRIBUTES");

            if (projections.length == 1) {
                scanExpression.withProjectionExpression(projections[0]);
            } else {
                scanExpression.withProjectionExpression(String.join(", ", projections));
            }
        }
    }

    @SuppressWarnings("Duplicates")
    private void setProjectionsOnQueryExpression(DynamoDBQueryExpression<E> queryExpression, String[] projections) {
        if (projections != null && projections.length > 0) {
            queryExpression.withSelect("SPECIFIC_ATTRIBUTES");

            if (projections.length == 1) {
                queryExpression.withProjectionExpression(projections[0]);
            } else {
                queryExpression.withProjectionExpression(String.join(", ", projections));
            }
        }
    }

    private String setScanPageToken(int pageCount, int desiredCount, List<E> itemList,
                                    String GSI, String alternateIndex, boolean unFilteredIndex) {
        String pagingToken;

        if (pageCount > desiredCount) {
            E lastItem = itemList.get(itemList.size() == 0 ? 0 : itemList.size() - 1);

            Map<String, AttributeValue> lastEvaluatedKey = setLastKey(lastItem, GSI, alternateIndex);
            pagingToken = lastEvaluatedKey == null ? "END_OF_LIST" :
                    setPageToken(lastEvaluatedKey, GSI, unFilteredIndex, alternateIndex);

            if (logger.isDebugEnabled()) {
                logger.debug("Constructed pagingtoken: " + pagingToken);
            }
        } else {
            pagingToken = "END_OF_LIST";
        }

        return pagingToken;
    }

    private List<E> reduceByPageToken(List<E> allItems, String id) {
        final Optional<E> first = allItems.stream()
                .filter(item -> {
                    if (hashOnlyModel()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Id is: " + id + ", Hash is: " + item.getHash());
                        }

                        return item.getHash().equals(id);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Id is: " + id + ", Range is: " + item.getRange());
                        }

                        return item.getRange().equals(id);
                    }
                })
                .findFirst();

        if (first.isPresent()) {
            allItems = allItems.subList(allItems.indexOf(first.get()) + 1, allItems.size());
        }

        return allItems;
    }

    private boolean hashOnlyModel() {
        return IDENTIFIER == null || IDENTIFIER.equals("");
    }

    private String setPageToken(Map<String, AttributeValue> lastEvaluatedKey, String GSI,
                                boolean unFilteredIndex, String alternateIndex) {
        return dbParams.createNewPageToken(HASH_IDENTIFIER, IDENTIFIER, PAGINATION_IDENTIFIER,
                lastEvaluatedKey, GSI, GSI_KEY_MAP, unFilteredIndex ? null : alternateIndex);
    }

    private Map<String, AttributeValue> getTokenMap(String pageToken, String GSI, String index) {
        return dbParams.createPageTokenMap(pageToken, HASH_IDENTIFIER, IDENTIFIER, index, GSI, GSI_KEY_MAP);
    }

    private Map<String, AttributeValue> setLastKey(E lastKey, String GSI, String index) {
        Map<String, AttributeValue> keyMap = new HashMap<>();
        boolean hashOnlyModel = hashOnlyModel();

        keyMap.put(HASH_IDENTIFIER, new AttributeValue().withS(lastKey.getHash()));
        if (!hashOnlyModel) keyMap.put(IDENTIFIER, new AttributeValue().withS(lastKey.getRange()));

        if (index == null && !hashOnlyModel) {
            keyMap.put(PAGINATION_IDENTIFIER, db.getIndexValue(PAGINATION_IDENTIFIER, lastKey));
        } else if (!hashOnlyModel) {
            if (logger.isDebugEnabled()) {
                logger.debug("Fetching remoteIndex value!");
            }

            keyMap.put(index, db.getIndexValue(index, lastKey));
        }

        if (GSI != null) {
            final JsonObject keyObject = GSI_KEY_MAP.get(GSI);
            final String hash = keyObject.getString("hash");
            final String range = keyObject.getString("range");

            keyMap.putIfAbsent(hash, new AttributeValue().withS(db.getFieldAsString(hash, lastKey)));

            if (!hashOnlyModel) {
                keyMap.putIfAbsent(range, db.getIndexValue(range, lastKey));
            }
        }

        return keyMap;
    }

    public void readAllWithoutPagination(String identifier,
                                         Handler<AsyncResult<List<E>>> resultHandler) {
        vertx.<List<E>>executeBlocking(future -> {
            try {
                DynamoDBQueryExpression<E> queryExpression = new DynamoDBQueryExpression<>();
                E keyItem = TYPE.newInstance();
                keyItem.setHash(identifier);
                queryExpression.setHashKeyValues(keyItem);
                queryExpression.setConsistentRead(true);

                if (logger.isDebugEnabled()) { logger.debug("readAllWithoutPagination single id, hashObject: " + Json.encodePrettily(keyItem)); }

                long timeBefore = System.currentTimeMillis();

                PaginatedQueryList<E> items = DYNAMO_DB_MAPPER.query(TYPE, queryExpression);
                items.loadAllResults();

                if (logger.isDebugEnabled()) {
                    logger.debug(items.size() + " results received in: " +
                            (System.currentTimeMillis() - timeBefore) + " ms");
                }

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
                logger.error("Error in readAllWithoutPagination!", readResult.cause());

                resultHandler.handle(ServiceException.fail(500, "Error in readAllWithoutPagination!",
                        new JsonObject(Json.encode(readResult.cause()))));
            } else {
                resultHandler.handle(Future.succeededFuture(readResult.result()));
            }
        });
    }

    public void readAllWithoutPagination(String identifier, QueryPack queryPack,
                                         Handler<AsyncResult<List<E>>> resultHandler) {
        readAllWithoutPagination(identifier, queryPack, null, resultHandler);
    }

    @SuppressWarnings("unused")
    public void readAllWithoutPagination(QueryPack queryPack, String[] projections,
                                         Handler<AsyncResult<List<E>>> resultHandler) {
        readAllWithoutPagination(queryPack, projections, null, resultHandler);
    }

    public void readAllWithoutPagination(QueryPack queryPack, String[] projections,
                                         String GSI, Handler<AsyncResult<List<E>>> resultHandler) {
        Map<String, List<FilterParameter>> params = null;
        if (queryPack != null) params = queryPack.getParams();
        final Map<String, List<FilterParameter>> finalParams = params;

        if (logger.isDebugEnabled()) {
            logger.debug("Running aggregation non pagination scan!");
        }

        vertx.<List<E>>executeBlocking(future -> {
            try {
                DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
                if (finalParams != null) scanExpression = dbParams.applyParameters(finalParams);

                if (projections != null) {
                    setProjectionsOnScanExpression(scanExpression, projections);
                }

                long timeBefore = System.currentTimeMillis();

                if (GSI != null) {
                    scanExpression.setIndexName(GSI);
                    scanExpression.setConsistentRead(false);
                }

                PaginatedParallelScanList<E> items =
                        DYNAMO_DB_MAPPER.parallelScan(TYPE, scanExpression, coreNum);
                items.loadAllResults();

                if (logger.isDebugEnabled()) {
                    logger.debug(items.size() + " results received in: " +
                            (System.currentTimeMillis() - timeBefore) + " ms");
                }

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
                logger.error("Error in readAllWithoutPagination!", readResult.cause());

                resultHandler.handle(ServiceException.fail(500, "Error in readAll!",
                        new JsonObject(Json.encode(readResult.cause()))));
            } else {
                resultHandler.handle(Future.succeededFuture(readResult.result()));
            }
        });
    }

    @SuppressWarnings("WeakerAccess")
    public void readAllWithoutPagination(String identifier, QueryPack queryPack, String[] projections,
                                         Handler<AsyncResult<List<E>>> resultHandler) {
        readAllWithoutPagination(identifier, queryPack, projections, null, resultHandler);
    }

    public void readAllWithoutPagination(String identifier, QueryPack queryPack, String[] projections,
                                         String GSI, Handler<AsyncResult<List<E>>> resultHandler) {
        vertx.<List<E>>executeBlocking(future -> {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Running aggregation non pagination query with id: " + identifier);
                }

                Queue<OrderByParameter> orderByQueue = queryPack.getOrderByQueue();
                Map<String, List<FilterParameter>> params = queryPack.getParams();
                String indexName = queryPack.getIndexName();

                if (logger.isDebugEnabled()) { logger.debug("Building expression..."); }

                DynamoDBQueryExpression<E> filterExpression;

                if (params != null) {
                    filterExpression = dbParams.applyParameters(
                            projections == null && orderByQueue != null ? orderByQueue.peek() : null,
                            params);
                    if (projections == null) {
                        filterExpression = dbParams.applyOrderBy(orderByQueue, GSI, indexName, filterExpression);
                    }
                } else {
                    filterExpression = new DynamoDBQueryExpression<>();
                }

                if (GSI == null) {
                    if (filterExpression.getKeyConditionExpression() == null) {
                        E keyItem = TYPE.newInstance();
                        keyItem.setHash(identifier);
                        filterExpression.setHashKeyValues(keyItem);
                    }

                    filterExpression.setConsistentRead(true);
                } else {
                    filterExpression.setIndexName(GSI);

                    if (filterExpression.getKeyConditionExpression() == null) {
                        setFilterExpressionKeyCondition(filterExpression, GSI, identifier);
                    }
                }

                setProjectionsOnQueryExpression(filterExpression, projections);

                long timeBefore = System.currentTimeMillis();

                if (logger.isDebugEnabled()) {
                    logger.debug("Filter Expression: " + Json.encodePrettily(filterExpression));
                }

                PaginatedQueryList<E> items = DYNAMO_DB_MAPPER.query(TYPE, filterExpression);
                items.loadAllResults();

                if (logger.isDebugEnabled()) {
                    logger.debug(items.size() +
                            " results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                }

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
                logger.error("Error in readAllWithoutPagination!", readResult.cause());

                resultHandler.handle(ServiceException.fail(500, "Error in readAllWithoutPagination!",
                        new JsonObject(Json.encode(readResult.cause()))));
            } else {
                resultHandler.handle(Future.succeededFuture(readResult.result()));
            }
        });
    }

    private void setFilterExpressionKeyCondition(DynamoDBQueryExpression<E> filterExpression,
                                                 String GSI, String identifier)
            throws IllegalAccessException, InstantiationException {
        E keyItem = TYPE.newInstance();
        final String GSI_FIELD = GSI_KEY_MAP.get(GSI).getString("hash");

        if (logger.isDebugEnabled()) {
            logger.debug("Fetching field " + GSI_FIELD);
        }

        final Field field = db.getField(GSI_FIELD);

        if (logger.isDebugEnabled()) {
            logger.debug("Field collected " + field);
        }

        if (field != null) {
            field.set(keyItem, identifier);
        }

        filterExpression.setHashKeyValues(keyItem);
        filterExpression.setConsistentRead(false);
    }

    private String getPaginationIndex() {
        return PAGINATION_IDENTIFIER != null && !PAGINATION_IDENTIFIER.equals("") ? PAGINATION_INDEX : null;
    }
}
