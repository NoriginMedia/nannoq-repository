package com.nannoq.tools.repository.dynamodb.operators;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.nannoq.tools.repository.CacheManager;
import com.nannoq.tools.repository.RedisUtils;
import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.DynamoDBModel;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
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
import io.vertx.redis.RedisClient;
import io.vertx.serviceproxy.ServiceException;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.nannoq.tools.repository.Repository.MULTIPLE_KEY;
import static com.nannoq.tools.repository.dynamodb.DynamoDBRepository.PAGINATION_INDEX;
import static java.util.stream.Collectors.toList;

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

    private final int coreNum = Runtime.getRuntime().availableProcessors() * 2;

    private final RedisClient REDIS_CLIENT;

    public DynamoDBReader(Class<E> type, Vertx vertx, DynamoDBRepository<E> db, String COLLECTION,
                          String HASH_IDENTIFIER, String IDENTIFIER, String PAGINATION_IDENTIFIER,
                          Map<String, JsonObject> GSI_KEY_MAP,
                          DynamoDBParameters<E> dbParams, CacheManager<E> cacheManager) {
        TYPE = type;
        this.vertx = vertx;
        this.db = db;
        this.COLLECTION = COLLECTION;
        this.HASH_IDENTIFIER = HASH_IDENTIFIER;
        this.IDENTIFIER = IDENTIFIER;
        this.PAGINATION_IDENTIFIER = PAGINATION_IDENTIFIER;
        this.DYNAMO_DB_MAPPER = db.getDynamoDbMapper();
        this.REDIS_CLIENT = db.getRedisClient();
        this.GSI_KEY_MAP = GSI_KEY_MAP;
        this.dbParams = dbParams;
        this.cacheManager = cacheManager;
    }

    public void read(JsonObject identifiers, Handler<AsyncResult<E>> resultHandler) {
        String hash = identifiers.getString("hash");
        String range = identifiers.getString("range");
        String cacheBase = TYPE.getSimpleName() + "_" + hash + (range == null ? "" : "/" + range);
        String cacheId = "FULL_CACHE_" + cacheBase;

        vertx.<E>executeBlocking(future -> cacheManager.checkObjectCache(cacheId, result -> {
            if (result.failed()) {
                future.fail(result.cause());
            } else {
                future.complete(result.result());
            }
        }), false, checkResult -> {
            if (checkResult.succeeded()) {
                resultHandler.handle(Future.succeededFuture(checkResult.result()));

                if (logger.isDebugEnabled()) { logger.debug("Served cached version of: " + cacheId); }
            } else {
                vertx.<E>executeBlocking(future -> {
                    E item = null;

                    try {
                        if (range != null && db.hasRangeKey()) {
                            long timeBefore = System.currentTimeMillis();

                            item = DYNAMO_DB_MAPPER.load(TYPE, hash, range);

                            if (logger.isDebugEnabled()) {
                                logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                            }
                        } else {
                            DynamoDBQueryExpression<E> query =
                                    new DynamoDBQueryExpression<>();
                            E keyObject = TYPE.newInstance();
                            keyObject.setHash(hash);
                            query.setConsistentRead(true);
                            query.setHashKeyValues(keyObject);
                            query.setLimit(1);

                            long timeBefore = System.currentTimeMillis();

                            List<E> items = DYNAMO_DB_MAPPER.query(TYPE, query);

                            if (logger.isDebugEnabled()) {
                                logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                            }

                            if (!items.isEmpty()) item = items.get(0);
                        }
                    } catch (Exception e) {
                        logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));
                    }

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
                        if (readResult.cause().getClass() == NoSuchElementException.class) {
                            logger.debug("Not found!");

                            resultHandler.handle(ServiceException.fail(404, "Not found!",
                                    new JsonObject(Json.encode(readResult.cause()))));
                        } else {
                            logger.error("Error in read!", readResult.cause());

                            resultHandler.handle(ServiceException.fail(500, "Error in read!",
                                    new JsonObject(Json.encode(readResult.cause()))));
                        }
                    } else {
                        resultHandler.handle(Future.succeededFuture(readResult.result()));
                    }
                });
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void read(JsonObject identifiers, boolean consistent, String[] projections,
                     Handler<AsyncResult<E>> resultHandler) {
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
                resultHandler.handle(Future.succeededFuture(checkResult.result()));

                if (logger.isDebugEnabled()) { logger.debug("Served cached version of: " + cacheId); }
            } else {
                vertx.<E>executeBlocking(future -> {
                    E item = null;

                    try {
                        if (range != null && db.hasRangeKey()) {
                            long timeBefore = System.currentTimeMillis();

                            item = DYNAMO_DB_MAPPER.load(TYPE, hash, range);

                            if (logger.isDebugEnabled()) {
                                logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                            }
                        } else {
                            DynamoDBQueryExpression<E> query =
                                    new DynamoDBQueryExpression<>();
                            E keyObject = TYPE.newInstance();
                            keyObject.setHash(hash);
                            query.setConsistentRead(consistent);
                            query.setHashKeyValues(keyObject);
                            query.setLimit(1);

                            if (projections != null && projections.length > 0) {
                                query.withSelect("SPECIFIC_ATTRIBUTES");

                                if (projections.length == 1) {
                                    query.withProjectionExpression(projections[0]);
                                } else {
                                    query.withProjectionExpression(String.join(", ", projections));
                                }
                            }

                            long timeBefore = System.currentTimeMillis();

                            List<E> items = DYNAMO_DB_MAPPER.query(TYPE, query);

                            if (logger.isDebugEnabled()) {
                                logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                            }

                            if (!items.isEmpty()) item = items.get(0);
                        }
                    } catch (Exception e) {
                        logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));
                    }

                    if (item != null) {
                        item.generateAndSetEtag(new ConcurrentHashMap<>());
                    }

                    if (projections != null && projections.length > 0 && item != null) {
                        String etagKeyBase = TYPE.getSimpleName() + "_" + hash + "/projections";
                        String key = TYPE.getSimpleName() + "_" + hash + "/projections" + Arrays.hashCode(projections);
                        String etag = item.getEtag();

                        RedisUtils.performJedisWithRetry(REDIS_CLIENT, in -> in.hset(etagKeyBase, key, etag, setRes -> {
                            if (setRes.failed()) {
                                logger.error("Unable to store projection etag!", setRes.cause());
                            }
                        }));
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
                        logger.error("Error in Read!", readResult.cause());

                        resultHandler.handle(ServiceException.fail(500, "Error in remoteRead!",
                                new JsonObject(Json.encode(readResult.cause()))));
                    } else {
                        resultHandler.handle(Future.succeededFuture(readResult.result()));
                    }
                });
            }
        });
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

    public void readAll(JsonObject identifiers, Map<String, List<FilterParameter<E>>> filterParameterMap,
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

    public void readAll(JsonObject identifiers, String pageToken, QueryPack<E> queryPack, String[] projections,
                        Handler<AsyncResult<ItemList<E>>> resultHandler) {
        readAll(identifiers, pageToken, queryPack, projections, null, resultHandler);
    }

    public void readAll(JsonObject identifiers, String pageToken, QueryPack<E> queryPack, String[] projections,
                        String GSI, Handler<AsyncResult<ItemList<E>>> resultHandler) {
        String hash = identifiers.getString("hash");
        String cacheId = TYPE.getSimpleName() + "_" + hash +
                (queryPack.getQuery() != null ? "/" + queryPack.getQuery() : "/START");

        if (logger.isDebugEnabled()) { logger.debug("Running readAll with: " + hash + " : " + cacheId); }

        vertx.<ItemList<E>>executeBlocking(future -> cacheManager.checkItemListCache(cacheId, projections, result -> {
            if (result.failed()) {
                future.fail(result.cause());
            } else {
                future.complete(result.result());
            }
        }), false, checkResult -> {
            if (checkResult.succeeded()) {
                resultHandler.handle(Future.succeededFuture(checkResult.result()));

                if (logger.isDebugEnabled()) { logger.debug("Served cached version of: " + cacheId); }
            } else {
                Queue<OrderByParameter> orderByQueue = queryPack.getOrderByQueue();
                Map<String, List<FilterParameter<E>>> params = queryPack.getParams();
                String indexName = queryPack.getIndexName();
                Integer limit = queryPack.getLimit();

                if (logger.isDebugEnabled()) {
                    logger.debug("Building expression...");
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
                        filterExpression, projections, GSI, resultHandler);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void returnDatabaseContent(QueryPack<E> queryPack, JsonObject identifiers, String pageToken,
                                       String hash, String etagKey,
                                       String cacheId, DynamoDBQueryExpression<E> filteringExpression,
                                       String[] projections, String GSI,
                                       Handler<AsyncResult<ItemList<E>>> resultHandler) {
        vertx.<ItemList<E>>executeBlocking(future -> {
            Boolean multiple = identifiers.getBoolean(MULTIPLE_KEY);
            boolean unFilteredIndex = filteringExpression == null;
            String alternateIndex = null;
            int count;
            List<E> itemList;
            Map<String, AttributeValue> lastEvaluatedKey = null;
            Map<String, List<FilterParameter<E>>> params = queryPack.getParams();
            List<FilterParameter<E>> nameParams = params == null ? null : params.get(PAGINATION_IDENTIFIER);

            if (logger.isDebugEnabled()) { logger.debug("Do remoteRead"); }

            try {
                if (filteringExpression != null) {
                    alternateIndex = db.getAlternativeIndexIdentifier(filteringExpression.getIndexName());
                }

                String[] effectivelyFinalProjections = dbParams.buildProjections(projections,
                        (identifiers.isEmpty() || filteringExpression == null) ?
                                PAGINATION_INDEX : alternateIndex);

                if (identifiers.isEmpty()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Running root query...");
                    }

                    DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
                    scanExpression.setLimit(20);
                    scanExpression.setIndexName(GSI != null ? GSI : PAGINATION_INDEX);
                    scanExpression.setConsistentRead(false);
                    scanExpression.setExclusiveStartKey(getTokenMap(pageToken, GSI, PAGINATION_IDENTIFIER));

                    if (effectivelyFinalProjections != null && effectivelyFinalProjections.length > 0) {
                        scanExpression.withSelect("SPECIFIC_ATTRIBUTES");

                        if (effectivelyFinalProjections.length == 1) {
                            scanExpression.withProjectionExpression(effectivelyFinalProjections[0]);
                        } else {
                            scanExpression.withProjectionExpression(String.join(", ", effectivelyFinalProjections));
                        }
                    }

                    long timeBefore = System.currentTimeMillis();

                    ScanResultPage<E> scanPageResults =
                            DYNAMO_DB_MAPPER.scanPage(TYPE, scanExpression);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                    }

                    count = scanPageResults.getCount();
                    itemList = scanPageResults.getResults();
                    lastEvaluatedKey = scanPageResults.getLastEvaluatedKey();
                } else if (params != null && nameParams != null && dbParams.isIllegalRangedKeyQueryParams(nameParams)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Running illegal rangedKey query...");
                    }

                    String hashScanKey = "#HASH_KEY_VALUE";
                    String hashScanValue = ":HASH_VALUE";

                    DynamoDBScanExpression scanExpression = dbParams.applyParameters(params);
                    String conditionString = scanExpression.getFilterExpression();
                    if (hash != null) conditionString += " AND " + hashScanKey + " = " + hashScanValue;
                    scanExpression.setFilterExpression(conditionString);

                    if (GSI == null) {
                        scanExpression.getExpressionAttributeNames().put(hashScanKey, HASH_IDENTIFIER);
                    } else {
                        scanExpression.getExpressionAttributeNames().put(hashScanKey, GSI_KEY_MAP.get(GSI)
                                .getString("hash"));
                    }

                    scanExpression.getExpressionAttributeValues().put(hashScanValue, new AttributeValue().withS(hash));

                    final Map<String, AttributeValue> pageTokenMap = getTokenMap(pageToken, GSI, PAGINATION_IDENTIFIER);

                    scanExpression.setLimit(null);
                    scanExpression.setIndexName(GSI != null ? GSI : PAGINATION_INDEX);
                    scanExpression.setConsistentRead(false);
                    scanExpression.setExclusiveStartKey(pageTokenMap);

                    if (projections != null && projections.length > 0) {
                        scanExpression.withSelect("SPECIFIC_ATTRIBUTES");

                        if (projections.length == 1) {
                            scanExpression.withProjectionExpression(projections[0]);
                        } else {
                            scanExpression.withProjectionExpression(String.join(", ", projections));
                        }
                    }

                    long timeBefore = System.currentTimeMillis();

                    if (logger.isDebugEnabled()) {
                        logger.debug("Scan expression is: " + Json.encodePrettily(scanExpression));
                    }

                    ScanResultPage<E> items =
                            DYNAMO_DB_MAPPER.scanPage(TYPE, scanExpression);
                    int pageCount = items.getCount();
                    int desiredCount = scanExpression != null && scanExpression.getLimit() != null ?
                            scanExpression.getLimit() : 20;

                    if (logger.isDebugEnabled()) {
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

                    itemList = items.getResults().stream()
                            .sorted(comparing)
                            .limit(desiredCount)
                            .collect(toList());
                    count = pageCount < desiredCount ? pageCount : desiredCount;

                    if (pageCount > desiredCount) {
                        E lastItem = itemList.get(itemList.size() == 0 ? 0 : itemList.size() - 1);

                        lastEvaluatedKey = setLastKey(lastItem, GSI, alternateIndex);
                    }
                } else {
                    DynamoDBQueryExpression<E> queryExpression;

                    if (multiple != null && multiple) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Running multiple id query...");
                        }

                        List<String> multipleIds = identifiers.getJsonArray("range").stream()
                                .map(Object::toString)
                                .collect(toList());

                        int desiredCount = filteringExpression != null && filteringExpression.getLimit() != null ?
                                filteringExpression.getLimit() : 20;
                        List<KeyPair> keyPairsList = multipleIds.stream()
                                .distinct()
                                .map(id -> new KeyPair().withHashKey(hash).withRangeKey(id))
                                .collect(toList());

                        Map<Class<?>, List<KeyPair>> keyPairs = new HashMap<>();
                        keyPairs.put(TYPE, keyPairsList);

                        if (logger.isDebugEnabled()) { logger.debug("Keypairs: " + Json.encodePrettily(keyPairs)); }

                        long timeBefore = System.currentTimeMillis();

                        Map<String, List<Object>> items = DYNAMO_DB_MAPPER.batchLoad(keyPairs);

                        if (logger.isDebugEnabled()) {
                            logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                        }

                        if (logger.isDebugEnabled()) { logger.debug("Items Returned for collection: " + items.size()); }

                        itemList = items.get(COLLECTION).stream()
                                .map(item -> (E) item)
                                .sorted(Comparator.comparing(DynamoDBModel::getRange,
                                        Comparator.comparingInt(multipleIds::indexOf)))
                                .limit(desiredCount)
                                .collect(toList());

                        count = itemList.size() > desiredCount ? desiredCount : itemList.size();
                        E lastKey = itemList.size() == 0 ? null : itemList.size() < desiredCount ?
                                itemList.get(itemList.size() - 1) :
                                itemList.get(desiredCount - 1);

                        if (lastKey != null) {
                            lastEvaluatedKey = setLastKey(lastKey, GSI, PAGINATION_IDENTIFIER);
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Running standard query...");
                        }

                        if (filteringExpression == null) {
                            queryExpression = new DynamoDBQueryExpression<>();
                            queryExpression.setIndexName(GSI != null ? GSI : PAGINATION_INDEX);
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
                                    field.set(keyItem, hash);
                                }

                                queryExpression.setHashKeyValues(keyItem);
                                queryExpression.setConsistentRead(false);
                            }
                        }

                        queryExpression.setConsistentRead(false);

                        int desiredCount = 0;

                        if (!unFilteredIndex) {
                            desiredCount = queryExpression.getLimit();
                            queryExpression.setLimit(null);
                        }

                        if (effectivelyFinalProjections != null && effectivelyFinalProjections.length > 0) {
                            queryExpression.withSelect("SPECIFIC_ATTRIBUTES");

                            if (effectivelyFinalProjections.length == 1) {
                                queryExpression.withProjectionExpression(effectivelyFinalProjections[0]);
                            } else {
                                queryExpression.withProjectionExpression(String.join(", ", effectivelyFinalProjections));

                            }
                        }

                        if (logger.isDebugEnabled()) {
                            logger.debug(Json.encodePrettily(queryExpression));
                        }

                        long timeBefore = System.currentTimeMillis();

                        QueryResultPage<E> queryPageResults =
                                DYNAMO_DB_MAPPER.queryPage(TYPE, queryExpression);

                        if (logger.isDebugEnabled()) {
                            logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                        }

                        if (logger.isDebugEnabled()) {
                            logger.debug("Scanned items: " + queryPageResults.getScannedCount());
                        }

                        if (!unFilteredIndex) {
                            int pageCount = queryPageResults.getCount();
                            itemList = queryPageResults.getResults()
                                    .subList(0, pageCount < desiredCount ? pageCount : desiredCount);
                            count = pageCount < desiredCount ? pageCount : desiredCount;

                            if (pageCount > desiredCount) {
                                E lastItem = itemList.get(itemList.size() == 0 ? 0 : itemList.size() - 1);

                                lastEvaluatedKey = setLastKey(lastItem, GSI, alternateIndex);
                            }
                        } else {
                            count = queryPageResults.getCount();
                            itemList = queryPageResults.getResults();
                            lastEvaluatedKey = queryPageResults.getLastEvaluatedKey();
                        }
                    }
                }

                String pagingToken = lastEvaluatedKey == null ? "END_OF_LIST" :
                        setPageToken(lastEvaluatedKey, GSI, unFilteredIndex, alternateIndex);

                if (logger.isDebugEnabled()) { logger.debug("Constructed pagingtoken: " + pagingToken); }

                ItemList<E> returnList = new ItemList<>(pagingToken, count, itemList, projections);

                if (logger.isDebugEnabled()) { logger.debug("Constructed itemList!"); }
                Future<Boolean> itemListCacheFuture = Future.future();

                if (cacheManager.isItemListCacheAvailable()) {
                    if (logger.isDebugEnabled()) { logger.debug("Constructing cache!"); }

                    JsonObject cacheObject = returnList.toJson(projections);

                    if (logger.isDebugEnabled()) { logger.debug("Cache complete!"); }

                    String content = cacheObject.encode();

                    if (logger.isDebugEnabled()) { logger.debug("Cache encoded!"); }

                    cacheManager.replaceItemListCache(content, () -> cacheId, cacheRes -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Setting: " + etagKey + " with: " + returnList.getEtag());
                        }

                        String etagItemListHashKey = TYPE.getSimpleName() + "_" +
                                (hash != null ? hash + "_" : "") +
                                "itemListEtags";

                        RedisUtils.performJedisWithRetry(REDIS_CLIENT, in ->
                                in.hset(etagItemListHashKey, etagKey, returnList.getEtag(), setRes -> {
                                    itemListCacheFuture.complete(Boolean.TRUE);
                                }));
                    });
                } else {
                    itemListCacheFuture.complete();
                }

                itemListCacheFuture.setHandler(res -> future.complete(returnList));
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

    private String setPageToken(Map<String, AttributeValue> lastEvaluatedKey, String GSI,
                                boolean unFilteredIndex, String alternateIndex) {
        if (GSI == null) {
            return dbParams.createNewPageToken(HASH_IDENTIFIER, IDENTIFIER, PAGINATION_IDENTIFIER,
                    lastEvaluatedKey, unFilteredIndex ? null : alternateIndex);
        } else {
            final JsonObject keyObject = GSI_KEY_MAP.get(GSI);
            final String hash = keyObject.getString("hash");
            final String range = keyObject.getString("range");

            return dbParams.createNewPageToken(hash, range, PAGINATION_IDENTIFIER,
                    lastEvaluatedKey, unFilteredIndex ? null : alternateIndex);
        }
    }

    private Map<String, AttributeValue> getTokenMap(String pageToken, String GSI, String index) {
        if (GSI == null) {
            return dbParams.createPageTokenMap(pageToken,
                    HASH_IDENTIFIER, IDENTIFIER, index);
        } else {
            final JsonObject keyObject = GSI_KEY_MAP.get(GSI);
            final String hash = keyObject.getString("hash");
            final String range = keyObject.getString("range");

            return dbParams.createPageTokenMap(pageToken, hash, range, index);
        }
    }

    private Map<String, AttributeValue> setLastKey(E lastKey, String GSI, String index) {
        Map<String, AttributeValue> keyMap = new HashMap<>();

        if (GSI == null) {
            keyMap.put(HASH_IDENTIFIER, new AttributeValue().withS(lastKey.getHash()));
            keyMap.put(IDENTIFIER, new AttributeValue().withS(lastKey.getRange()));

            if (index == null) {
                keyMap.put(PAGINATION_IDENTIFIER, db.getIndexValue(PAGINATION_IDENTIFIER, lastKey));
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Fetching remoteIndex value!");
                }

                keyMap.put(index, db.getIndexValue(index, lastKey));
            }
        } else {
            final JsonObject keyObject = GSI_KEY_MAP.get(GSI);
            final String hash = keyObject.getString("hash");
            final String range = keyObject.getString("range");

            keyMap.put(hash, new AttributeValue().withS(lastKey.getHash()));
            keyMap.put(range, new AttributeValue().withS(lastKey.getRange()));
            keyMap.put(PAGINATION_IDENTIFIER, db.getIndexValue(PAGINATION_IDENTIFIER, lastKey));
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

    public void readAllWithoutPagination(String identifier, QueryPack<E> queryPack,
                                         Handler<AsyncResult<List<E>>> resultHandler) {
        readAllWithoutPagination(identifier, queryPack, null, resultHandler);
    }

    public void readAllWithoutPagination(QueryPack<E> queryPack, String[] projections,
                                         Handler<AsyncResult<List<E>>> resultHandler) {
        readAllWithoutPagination(queryPack, projections, null, resultHandler);
    }

    public void readAllWithoutPagination(QueryPack<E> queryPack, String[] projections,
                                         String GSI, Handler<AsyncResult<List<E>>> resultHandler) {
        Map<String, List<FilterParameter<E>>> params = null;
        if (queryPack != null) params = queryPack.getParams();
        final Map<String, List<FilterParameter<E>>> finalParams = params;

        if (logger.isDebugEnabled()) {
            logger.debug("Running aggregation non pagination scan with id!");
        }

        vertx.<List<E>>executeBlocking(future -> {
            try {
                DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
                if (finalParams != null) scanExpression = dbParams.applyParameters(finalParams);

                if (projections != null) {
                    scanExpression.withSelect("SPECIFIC_ATTRIBUTES");

                    if (projections.length == 1) {
                        scanExpression.withProjectionExpression(projections[0]);
                    } else {
                        scanExpression.withProjectionExpression(String.join(", ", projections));

                    }
                }

                long timeBefore = System.currentTimeMillis();

                if (GSI != null) {
                    scanExpression.setIndexName(GSI_KEY_MAP.get(GSI).getString("hash"));
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

    public void readAllWithoutPagination(String identifier, QueryPack<E> queryPack, String[] projections,
                                         Handler<AsyncResult<List<E>>> resultHandler) {
        readAllWithoutPagination(identifier, queryPack, projections, null, resultHandler);
    }

    public void readAllWithoutPagination(String identifier, QueryPack<E> queryPack, String[] projections,
                                         String GSI, Handler<AsyncResult<List<E>>> resultHandler) {
        vertx.<List<E>>executeBlocking(future -> {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Running aggregation non pagination query with id: " + identifier);
                }

                Queue<OrderByParameter> orderByQueue = queryPack.getOrderByQueue();
                Map<String, List<FilterParameter<E>>> params = queryPack.getParams();
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
                    if (filterExpression.getKeyConditionExpression() == null) {
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
                }

                if (projections != null) {
                    filterExpression.withSelect("SPECIFIC_ATTRIBUTES");

                    if (projections.length == 1) {
                        filterExpression.withProjectionExpression(projections[0]);
                    } else {
                        filterExpression.withProjectionExpression(String.join(", ", projections));

                    }
                }

                long timeBefore = System.currentTimeMillis();

                if (logger.isDebugEnabled()) {
                    logger.debug("Filter Expression: " + Json.encodePrettily(filterExpression));
                }

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
                logger.error("Error in readAllWithoutPagination!", readResult.cause());

                resultHandler.handle(ServiceException.fail(500, "Error in readAllWithoutPagination!",
                        new JsonObject(Json.encode(readResult.cause()))));
            } else {
                resultHandler.handle(Future.succeededFuture(readResult.result()));
            }
        });
    }
}
