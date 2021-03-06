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

import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import com.nannoq.tools.repository.models.*;
import com.nannoq.tools.repository.repository.cache.CacheManager;
import com.nannoq.tools.repository.repository.etag.ETagManager;
import com.nannoq.tools.repository.utils.AggregateFunction;
import com.nannoq.tools.repository.utils.GroupingConfiguration;
import com.nannoq.tools.repository.utils.QueryPack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

/**
 * This class defines the aggregate operations for the DynamoDBRepository.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@SuppressWarnings("Convert2MethodRef")
public class DynamoDBAggregates<E extends DynamoDBModel & Model & ETagable & Cacheable> {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBAggregates.class.getSimpleName());

    private final Class<E> TYPE;
    private final DynamoDBRepository<E> db;

    private final String HASH_IDENTIFIER;
    private final String IDENTIFIER;

    private final CacheManager<E> cacheManager;
    private final ETagManager<E> eTagManager;

    public DynamoDBAggregates(Class<E> TYPE, DynamoDBRepository<E> db,
                              String HASH_IDENTIFIER, String IDENTIFIER,
                              CacheManager<E> cacheManager, ETagManager<E> eTagManager) {
        this.TYPE = TYPE;
        this.db = db;
        this.HASH_IDENTIFIER = HASH_IDENTIFIER;
        this.IDENTIFIER = IDENTIFIER;
        this.cacheManager = cacheManager;
        this.eTagManager = eTagManager;
    }
    
    public void aggregation(JsonObject identifiers, QueryPack queryPack, String[] projections,
                            String GSI, Handler<AsyncResult<String>> resultHandler) {
        if (logger.isDebugEnabled()) { logger.debug("QueryPack is: " + Json.encodePrettily(queryPack) + ", projections: " +
                Arrays.toString(projections) + ", ids: " + identifiers.encodePrettily()); }

        switch (queryPack.getAggregateFunction().getFunction()) {
            case MIN:
                findItemsWithMinOfField(identifiers, queryPack, projections, GSI, resultHandler);

                break;
            case MAX:
                findItemsWithMaxOfField(identifiers, queryPack, projections, GSI, resultHandler);

                break;
            case AVG:
                avgField(identifiers, queryPack, GSI, resultHandler);

                break;
            case SUM:
                sumField(identifiers, queryPack, GSI, resultHandler);

                break;
            case COUNT:
                countItems(identifiers, queryPack, GSI, resultHandler);

                break;
        }
    }

    private void findItemsWithMinOfField(JsonObject identifiers, QueryPack queryPack,
                                         String[] projections, String GSI, Handler<AsyncResult<String>> resultHandler) {
        performMinOrMaxAggregation(identifiers, queryPack, "MIN",
                (r, f) -> getAllItemsWithLowestValue(r, f), projections, GSI, resultHandler);
    }

    @SuppressWarnings("unchecked")
    private void findItemsWithMaxOfField(JsonObject identifiers, QueryPack queryPack,
                                         String[] projections, String GSI, Handler<AsyncResult<String>> resultHandler) {
        performMinOrMaxAggregation(identifiers, queryPack, "MAX",
                (r, f) -> getAllItemsWithHighestValue(r, f), projections, GSI, resultHandler);
    }

    private void doIdentifierBasedQuery(JsonObject identifiers, QueryPack queryPack, String GSI,
                                        Handler<AsyncResult<List<E>>> res, String[][] projs) {
        if (identifiers.isEmpty()) {
            if (GSI != null) {
                db.readAllWithoutPagination(queryPack, addIdentifiers(projs[0]), GSI, res);
            } else {
                db.readAllWithoutPagination(queryPack, addIdentifiers(projs[0]), res);
            }
        } else {
            if (GSI != null) {
                db.readAllWithoutPagination(identifiers.getString("hash"), queryPack, addIdentifiers(projs[0]), GSI, res);
            } else {
                db.readAllWithoutPagination(identifiers.getString("hash"), queryPack, addIdentifiers(projs[0]), res);
            }
        }
    }

    private void calculateGroupingPageToken(List<GroupingConfiguration> groupingParam, String[][] projs, String[] finalProjections) {
        if (groupingParam != null) {
            groupingParam.stream()
                    .filter(param -> Arrays.stream(finalProjections).noneMatch(p -> p.equals(param.getGroupBy())))
                    .forEach(groupByParam -> {
                        String[] newProjectionArray = new String[finalProjections.length + 1];
                        IntStream.range(0, finalProjections.length).forEach(i -> newProjectionArray[i] = finalProjections[i]);
                        newProjectionArray[finalProjections.length] = groupByParam.getGroupBy();
                        projs[0] = newProjectionArray;
                    });
        }
    }

    private JsonObject calculateGroupings(AggregateFunction aggregateFunction, List<E> minItems) {
        return performGroupingAndSorting(minItems, aggregateFunction, (items, groupingConfigurations) -> {
            if (groupingConfigurations.size() > 3) throw new IllegalArgumentException("GroupBy size of three is max!");
            GroupingConfiguration levelOne = groupingConfigurations.get(0);
            GroupingConfiguration levelTwo = groupingConfigurations.size() > 1 ? groupingConfigurations.get(1) : null;
            GroupingConfiguration levelThree = groupingConfigurations.size() > 2 ? groupingConfigurations.get(2) : null;

            if (levelOne != null && levelTwo == null) {
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne)));
            } else if (levelOne != null && levelThree == null) {
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                groupingBy(item -> calculateGroupingKey(item, levelTwo))));
            } else if (levelThree != null) {
                //noinspection ConstantConditions
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                groupingBy(item -> calculateGroupingKey(item, levelTwo),
                                        groupingBy(item -> calculateGroupingKey(item, levelThree)))));
            }

            throw new IllegalArgumentException();
        });
    }

    private List<E> getAllItemsWithLowestValue(List<E> records, String field) {
        final List<E> result = new ArrayList<>();
        final List<E> min = new ArrayList<>();
        min.add(null);

        records.forEach(r -> {
            if (min.get(0) == null || db.extractValueAsDouble(db.checkAndGetField(field), r)
                    .compareTo(db.extractValueAsDouble(db.checkAndGetField(field), min.get(0))) < 0) {
                min.set(0, r);
                result.clear();
                result.add(r);
            } else if (min.get(0) != null && db.extractValueAsDouble(db.checkAndGetField(field), r)
                    .compareTo(db.extractValueAsDouble(db.checkAndGetField(field), min.get(0))) == 0) {
                result.add(r);
            }
        });

        return result;
    }

    private void performMinOrMaxAggregation(JsonObject identifiers, QueryPack queryPack, String command,
                                            BiFunction<List<E>, String, List<E>> valueExtractor, String[] projections,
                                            String GSI, Handler<AsyncResult<String>> resultHandler) {
        int hashCode = queryPack.getAggregateFunction().getGroupBy() == null ?
                0 : queryPack.getAggregateFunction().getGroupBy().hashCode();
        AggregateFunction aggregateFunction = queryPack.getAggregateFunction();
        String field = aggregateFunction.getField();
        String newEtagKeyPostfix = "_" + field + "_" + command;
        String etagKey = queryPack.getBaseEtagKey() + newEtagKeyPostfix + hashCode;
        String cacheKey = queryPack.getBaseEtagKey() + newEtagKeyPostfix + hashCode;
        final List<GroupingConfiguration> groupingParam = queryPack.getAggregateFunction().getGroupBy();

        cacheManager.checkAggregationCache(cacheKey, cacheRes -> {
            if (cacheRes.failed()) {
                final Handler<AsyncResult<List<E>>> res = allResult -> {
                    if (allResult.failed()) {
                        resultHandler.handle(Future.failedFuture("Could not remoteRead all records..."));
                    } else {
                        List<E> records = allResult.result();

                        if (records.size() == 0) {
                            setEtagAndCacheAndReturnContent(etagKey, identifiers.encode().hashCode(), cacheKey,
                                    new JsonObject().put("error", "Empty table!").encode(), resultHandler);
                        } else {
                            if (queryPack.getAggregateFunction().hasGrouping()) {
                                List<E> maxItems = getAllItemsWithHighestValue(allResult.result(), field);
                                JsonObject aggregatedItems = calculateGroupings(aggregateFunction, maxItems);

                                setEtagAndCacheAndReturnContent(etagKey, identifiers.encode().hashCode(), cacheKey, aggregatedItems.encode(), resultHandler);
                            } else {
                                JsonArray items = new JsonArray();
                                valueExtractor.apply(records, field).stream()
                                        .map(o -> o.toJsonFormat())
                                        .forEach(items::add);

                                setEtagAndCacheAndReturnContent(etagKey, identifiers.encode().hashCode(), cacheKey, items.encode(), resultHandler);
                            }
                        }
                    }
                };

                final String[][] projs = {projections};
                String[] finalProjections = projections == null ? new String[]{} : projections;

                calculateGroupingPageToken(groupingParam, projs, finalProjections);

                String[] finalProjections2 = projs[0] == null ? new String[]{} : projs[0];

                if (field != null) {
                    if (Arrays.stream(finalProjections2).noneMatch(p -> p.equalsIgnoreCase(field))) {
                        String[] newProjectionArray = new String[finalProjections2.length + 1];
                        IntStream.range(0, finalProjections2.length).forEach(i -> newProjectionArray[i] = finalProjections2[i]);
                        newProjectionArray[finalProjections2.length] = field;
                        projs[0] = newProjectionArray;
                    }
                }

                if (logger.isDebugEnabled()) { logger.debug("Projections: " + Arrays.toString(projs[0])); }

                doIdentifierBasedQuery(identifiers, queryPack, GSI, res, projs);
            } else {
                resultHandler.handle(Future.succeededFuture(cacheRes.result()));
            }
        });
    }

    private String[] addIdentifiers(String[] projections) {
        String[] projs = addHashIdentifierToProjections(projections);

        return addRangeIdentifierToProjections(projs);
    }

    private String[] addHashIdentifierToProjections(String[] projections) {
        if (HASH_IDENTIFIER != null && Arrays.stream(projections).noneMatch(p -> p.equalsIgnoreCase(HASH_IDENTIFIER))) {
            String[] newProjectionArray = new String[projections.length + 1];
            IntStream.range(0, projections.length).forEach(i -> newProjectionArray[i] = projections[i]);
            newProjectionArray[projections.length] = HASH_IDENTIFIER;

            return newProjectionArray;
        } else {
            return projections;
        }
    }

    private String[] addRangeIdentifierToProjections(String[] projections) {
        if (IDENTIFIER != null && Arrays.stream(projections).noneMatch(p -> p != null && p.equalsIgnoreCase(IDENTIFIER))) {
            String[] newProjectionArray = new String[projections.length + 1];
            IntStream.range(0, projections.length).forEach(i -> newProjectionArray[i] = projections[i]);
            newProjectionArray[projections.length] = IDENTIFIER;

            return newProjectionArray;
        } else {
            return projections;
        }
    }

    private List<E> getAllItemsWithHighestValue(List<E> records, String field) {
        final List<E> result = new ArrayList<>();
        final List<E> max = new ArrayList<>();
        max.add(null);

        records.forEach(r -> {
            if (max.get(0) == null || db.extractValueAsDouble(db.checkAndGetField(field), r)
                    .compareTo(db.extractValueAsDouble(db.checkAndGetField(field), max.get(0))) > 0) {
                max.set(0, r);
                result.clear();
                result.add(r);
            } else if (max.get(0) != null && db.extractValueAsDouble(db.checkAndGetField(field), r)
                    .compareTo(db.extractValueAsDouble(db.checkAndGetField(field), max.get(0))) == 0) {
                result.add(r);
            }
        });

        return result;
    }

    private void avgField(JsonObject identifiers, QueryPack queryPack, String GSI,
                          Handler<AsyncResult<String>> resultHandler) {
        int hashCode = queryPack.getAggregateFunction().getGroupBy() == null ?
                0 : queryPack.getAggregateFunction().getGroupBy().hashCode();
        AggregateFunction aggregateFunction = queryPack.getAggregateFunction();
        String field = aggregateFunction.getField();
        String newEtagKeyPostfix = "_" + field + "_AVG";
        String etagKey = queryPack.getBaseEtagKey() + newEtagKeyPostfix + hashCode;
        String cacheKey = queryPack.getBaseEtagKey() + newEtagKeyPostfix + hashCode;
        final List<GroupingConfiguration> groupingParam = queryPack.getAggregateFunction().getGroupBy();

        cacheManager.checkAggregationCache(cacheKey, cacheRes -> {
            if (cacheRes.failed()) {
                final Handler<AsyncResult<List<E>>> res = allResult -> {
                    if (allResult.failed()) {
                        resultHandler.handle(Future.failedFuture("Could not remoteRead all records..."));
                    } else {
                        List<E> records = allResult.result();

                        if (records.size() == 0) {
                            setEtagAndCacheAndReturnContent(etagKey, identifiers.encode().hashCode(), cacheKey,
                                    new JsonObject().put("error", "Empty table!").encode(), resultHandler);
                        } else {
                            JsonObject avg;

                            if (queryPack.getAggregateFunction().hasGrouping()) {
                                avg = avgGrouping(allResult.result(), aggregateFunction, field);
                            } else {
                                avg = new JsonObject();

                                records.stream()
                                        .mapToDouble(r -> db.extractValueAsDouble(db.checkAndGetField(field), r))
                                        .filter(Objects::nonNull)
                                        .average().ifPresent(value -> avg.put("avg", value));

                                if (avg.size() == 0) {
                                    avg.put("avg", 0.0);
                                }
                            }

                            setEtagAndCacheAndReturnContent(etagKey, identifiers.encode().hashCode(), cacheKey, avg.encode(), resultHandler);
                        }
                    }
                };

                final String[][] projections = {new String[]{field}};
                String[] finalProjections = projections[0];

                calculateGroupingPageToken(groupingParam, projections, finalProjections);

                doIdentifierBasedQueryNoIdentifierAddition(identifiers, queryPack, GSI, res, projections);
            } else {
                resultHandler.handle(Future.succeededFuture(cacheRes.result()));
            }
        });
    }

    private JsonObject avgGrouping(List<E> result, AggregateFunction aggregateFunction, String field) {
        return performGroupingAndSorting(result, aggregateFunction, (items, groupingConfigurations) -> {
            if (groupingConfigurations.size() > 3) throw new IllegalArgumentException("GroupBy size of three is max!");
            GroupingConfiguration levelOne = groupingConfigurations.get(0);
            GroupingConfiguration levelTwo = groupingConfigurations.size() > 1 ? groupingConfigurations.get(1) : null;
            GroupingConfiguration levelThree = groupingConfigurations.size() > 2 ? groupingConfigurations.get(2) : null;

            if (levelOne != null && levelTwo == null) {
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                averagingDouble(item -> db.extractValueAsDouble(db.checkAndGetField(field), item))));
            } else if (levelOne != null && levelThree == null) {
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                groupingBy(item -> calculateGroupingKey(item, levelTwo),
                                        averagingDouble(item -> db.extractValueAsDouble(db.checkAndGetField(field), item)))));
            } else if (levelThree != null) {
                //noinspection ConstantConditions
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                groupingBy(item -> calculateGroupingKey(item, levelTwo),
                                        groupingBy(item -> calculateGroupingKey(item, levelThree),
                                                summingDouble(item -> db.extractValueAsDouble(db.checkAndGetField(field), item))))));
            }

            throw new IllegalArgumentException();
        });
    }

    private void doIdentifierBasedQueryNoIdentifierAddition(JsonObject identifiers, QueryPack queryPack, String GSI,
                                                            Handler<AsyncResult<List<E>>> res, String[] projections) {
        String[][] temp = new String[1][1];
        temp[0] = projections;

        doIdentifierBasedQueryNoIdentifierAddition(identifiers, queryPack, GSI, res, temp);
    }

    private void doIdentifierBasedQueryNoIdentifierAddition(JsonObject identifiers, QueryPack queryPack, String GSI,
                                                            Handler<AsyncResult<List<E>>> res, String[][] projections) {
        if (identifiers.isEmpty()) {
            if (GSI != null) {
                db.readAllWithoutPagination(queryPack, projections[0], GSI, res);
            } else {
                db.readAllWithoutPagination(queryPack, projections[0], res);
            }
        } else {
            if (GSI != null) {
                db.readAllWithoutPagination(identifiers.getString("hash"), queryPack, projections[0], GSI, res);
            } else {
                db.readAllWithoutPagination(identifiers.getString("hash"), queryPack, projections[0], res);
            }
        }
    }

    private void sumField(JsonObject identifiers, QueryPack queryPack, String GSI,
                          Handler<AsyncResult<String>> resultHandler) {
        int hashCode = queryPack.getAggregateFunction().getGroupBy() == null ?
                0 : queryPack.getAggregateFunction().getGroupBy().hashCode();
        AggregateFunction aggregateFunction = queryPack.getAggregateFunction();
        String field = aggregateFunction.getField();
        String newEtagKeyPostfix = "_" + field + "_SUM";
        String etagKey = queryPack.getBaseEtagKey() + newEtagKeyPostfix + hashCode;
        String cacheKey = queryPack.getBaseEtagKey() + newEtagKeyPostfix + hashCode;
        final List<GroupingConfiguration> groupingParam = queryPack.getAggregateFunction().getGroupBy();

        cacheManager.checkAggregationCache(cacheKey, cacheRes -> {
            if (cacheRes.failed()) {
                final Handler<AsyncResult<List<E>>> res = allResult -> {
                    if (allResult.failed()) {
                        logger.error("Read all failed!", allResult.cause());

                        resultHandler.handle(Future.failedFuture("Could not remoteRead all records..."));
                    } else {
                        List<E> records = allResult.result();

                        if (records.size() == 0) {
                            setEtagAndCacheAndReturnContent(etagKey, identifiers.encode().hashCode(), cacheKey,
                                    new JsonObject().put("error", "Empty table!").encode(), resultHandler);
                        } else {
                            JsonObject sum = aggregateFunction.hasGrouping() ?
                                    sumGrouping(allResult.result(), aggregateFunction, field) :
                                    new JsonObject().put("sum", records.stream()
                                            .mapToDouble(r -> db.extractValueAsDouble(db.checkAndGetField(field), r))
                                            .filter(Objects::nonNull)
                                            .sum());

                            setEtagAndCacheAndReturnContent(etagKey, identifiers.encode().hashCode(), cacheKey, sum.encode(), resultHandler);
                        }
                    }
                };

                final String[][] projections = {new String[]{field}};
                String[] finalProjections = projections[0];

                calculateGroupingPageToken(groupingParam, projections, finalProjections);

                doIdentifierBasedQueryNoIdentifierAddition(identifiers, queryPack, GSI, res, projections);
            } else {
                resultHandler.handle(Future.succeededFuture(cacheRes.result()));
            }
        });
    }

    private JsonObject sumGrouping(List<E> result, AggregateFunction aggregateFunction, String field) {
        return performGroupingAndSorting(result, aggregateFunction, (items, groupingConfigurations) -> {
            if (groupingConfigurations.size() > 3) throw new IllegalArgumentException("GroupBy size of three is max!");
            GroupingConfiguration levelOne = groupingConfigurations.get(0);
            GroupingConfiguration levelTwo = groupingConfigurations.size() > 1 ? groupingConfigurations.get(1) : null;
            GroupingConfiguration levelThree = groupingConfigurations.size() > 2 ? groupingConfigurations.get(2) : null;

            if (levelOne != null && levelTwo == null) {
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                summingDouble(item -> db.extractValueAsDouble(db.checkAndGetField(field), item))));
            } else if (levelOne != null && levelThree == null) {
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                groupingBy(item -> calculateGroupingKey(item, levelTwo),
                                        summingDouble(item -> db.extractValueAsDouble(db.checkAndGetField(field), item)))));
            } else if (levelThree != null) {
                //noinspection ConstantConditions
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                groupingBy(item -> calculateGroupingKey(item, levelTwo),
                                        groupingBy(item -> calculateGroupingKey(item, levelThree),
                                                summingDouble(item -> db.extractValueAsDouble(db.checkAndGetField(field), item))))));
            }

            throw new IllegalArgumentException();
        });
    }

    private void countItems(JsonObject identifiers, QueryPack queryPack, String GSI,
                            Handler<AsyncResult<String>> resultHandler) {
        String newEtagKeyPostfix = "_COUNT";
        String etagKey = queryPack.getBaseEtagKey() +
                newEtagKeyPostfix + queryPack.getAggregateFunction().getGroupBy().hashCode();
        String cacheKey = queryPack.getBaseEtagKey() +
                newEtagKeyPostfix + queryPack.getAggregateFunction().getGroupBy().hashCode();

        cacheManager.checkAggregationCache(cacheKey, cacheRes -> {
            if (cacheRes.failed()) {
                final AggregateFunction aggregateFunction = queryPack.getAggregateFunction();

                final Handler<AsyncResult<List<E>>> res = allResult -> {
                    if (allResult.failed()) {
                        resultHandler.handle(Future.failedFuture("Could not remoteRead all records..."));
                    } else {
                        JsonObject count = aggregateFunction.hasGrouping() ?
                                countGrouping(allResult.result(), aggregateFunction) :
                                new JsonObject().put("count", allResult.result().size());

                        setEtagAndCacheAndReturnContent(etagKey, identifiers.encode().hashCode(), cacheKey, count.encode(), resultHandler);
                    }
                };

                String[] projections = !aggregateFunction.hasGrouping() ?
                        new String[]{"etag"} : aggregateFunction.getGroupBy().stream()
                        .map(GroupingConfiguration::getGroupBy)
                        .distinct()
                        .toArray(String[]::new);

                doIdentifierBasedQueryNoIdentifierAddition(identifiers, queryPack, GSI, res, projections);
            } else {
                resultHandler.handle(Future.succeededFuture(cacheRes.result()));
            }
        });
    }

    private JsonObject countGrouping(List<E> result, AggregateFunction aggregateFunction) {
        return performGroupingAndSorting(result, aggregateFunction, (items, groupingConfigurations) -> {
            if (groupingConfigurations.size() > 3) throw new IllegalArgumentException("GroupBy size of three is max!");
            GroupingConfiguration levelOne = groupingConfigurations.get(0);
            GroupingConfiguration levelTwo = groupingConfigurations.size() > 1 ? groupingConfigurations.get(1) : null;
            GroupingConfiguration levelThree = groupingConfigurations.size() > 2 ? groupingConfigurations.get(2) : null;

            if (levelOne != null && levelTwo == null) {
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                counting()));
            } else if (levelOne != null && levelThree == null) {
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                groupingBy(item -> calculateGroupingKey(item, levelTwo),
                                        counting())));
            } else if (levelThree != null) {
                //noinspection ConstantConditions
                return items.parallelStream()
                        .collect(groupingBy(item -> calculateGroupingKey(item, levelOne),
                                groupingBy(item -> calculateGroupingKey(item, levelTwo),
                                        groupingBy(item -> calculateGroupingKey(item, levelThree),
                                                counting()))));
            }

            throw new IllegalArgumentException();
        });
    }

    @SuppressWarnings("unchecked")
    private JsonObject performGroupingAndSorting(List<E> items, AggregateFunction aggregateFunction,
                                                 BiFunction<List<E>, List<GroupingConfiguration>, Map> mappingFunction) {
        List<GroupingConfiguration> groupingConfigurations = aggregateFunction.getGroupBy();
        if (groupingConfigurations.size() > 3) throw new IllegalArgumentException("GroupBy size of three is max!");
        GroupingConfiguration levelOne = groupingConfigurations.get(0);
        GroupingConfiguration levelTwo = groupingConfigurations.size() > 1 ? groupingConfigurations.get(1) : null;
        GroupingConfiguration levelThree = groupingConfigurations.size() > 2 ? groupingConfigurations.get(2) : null;
        Map collect = mappingFunction.apply(items, groupingConfigurations);

        String funcName = aggregateFunction.getFunction().name().toLowerCase();

        if (logger.isDebugEnabled()) {
            logger.debug("Map is: " + new JsonObject(collect).encodePrettily() + " with size: " + collect.size());
        }

        if (collect != null) {
            int totalGroupCount = collect.size();
            final Map levelOneStream;

            if (levelOne.hasGroupRanging()) {
                levelOneStream = doRangedSorting(collect, levelOne);
            } else {
                levelOneStream = doNormalSorting(collect, levelOne);
            }

            if (levelTwo == null) {
                if (levelOne.hasGroupRanging()) {
                    return doRangedGrouping(funcName, levelOneStream, levelOne, totalGroupCount);
                } else {
                    return doNormalGrouping(funcName, levelOneStream, totalGroupCount);
                }
            } else {
                final Stream<SimpleEntry> levelTwoStream = levelOneStream.entrySet().stream().map(e -> {
                    final Map.Entry entry = (Map.Entry) e;
                    final Map superGroupedItems = (Map) entry.getValue();
                    int totalSubGroupCount = superGroupedItems.size();

                    if (levelThree == null) {

                        if (levelTwo.hasGroupRanging()) {
                            return new SimpleEntry<>(entry.getKey(),
                                    doRangedGrouping(funcName,
                                            doRangedSorting(superGroupedItems, levelTwo), levelTwo, totalSubGroupCount));
                        } else {
                            return new SimpleEntry<>(entry.getKey(),
                                    doNormalGrouping(funcName,
                                            doNormalSorting(superGroupedItems, levelTwo), totalSubGroupCount));
                        }
                    } else {
                        final Map levelTwoMap;

                        if (levelTwo.hasGroupRanging()) {
                            levelTwoMap = doRangedSorting(superGroupedItems, levelTwo);
                        } else {
                            levelTwoMap = doNormalSorting(superGroupedItems, levelTwo);
                        }

                        final Stream<SimpleEntry> levelThreeStream = levelTwoMap.entrySet().stream().map(subE -> {
                            final Map.Entry subEntry = (Map.Entry) subE;
                            final Map subSuperGroupedItems = (Map) subEntry.getValue();

                            int totalSubSuperGroupCount = subSuperGroupedItems.size();

                            if (levelThree.hasGroupRanging()) {
                                return new SimpleEntry<>(subEntry.getKey(),
                                        doRangedGrouping(funcName, doRangedSorting(
                                                subSuperGroupedItems, levelThree), levelThree, totalSubSuperGroupCount));
                            } else {
                                return new SimpleEntry<>(subEntry.getKey(),
                                        doNormalGrouping(funcName, doNormalSorting(
                                                subSuperGroupedItems, levelThree), totalSubSuperGroupCount));
                            }
                        });

                        final Map levelThreeMap =
                                levelThreeStream.collect(toMap(
                                        SimpleEntry::getKey, SimpleEntry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

                        if (levelTwo.hasGroupRanging()) {
                            return new SimpleEntry<>(entry.getKey(),
                                    doRangedGrouping(funcName, levelThreeMap, levelTwo, totalSubGroupCount));
                        } else {
                            return new SimpleEntry<>(entry.getKey(),
                                    doNormalGrouping(funcName, levelThreeMap, totalSubGroupCount));
                        }
                    }
                });

                final Map levelTwoMap = levelTwoStream.collect(toMap(
                        SimpleEntry::getKey, SimpleEntry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

                if (levelOne.hasGroupRanging()) {
                    return doRangedGrouping(funcName, levelTwoMap, levelOne, totalGroupCount);
                } else {
                    return doNormalGrouping(funcName, levelTwoMap, totalGroupCount);
                }
            }
        } else {
            throw new InternalError();
        }
    }

    @SuppressWarnings("unchecked")
    private JsonObject doNormalGrouping(String aggregationFunctionKey, Map<String, Object> mapStream, int totalGroupCount) {
        JsonArray results = new JsonArray();
        mapStream.forEach((key, value) -> results.add(new JsonObject()
                .put("groupByKey", key)
                .put(aggregationFunctionKey, value)));

        return new JsonObject()
                .put("totalGroupCount", totalGroupCount)
                .put("count", results.size())
                .put("results", results);
    }

    @SuppressWarnings("unchecked")
    private JsonObject doRangedGrouping(String aggregationFunctionKey,
                                        Map<String, Object> mapStream,
                                        GroupingConfiguration groupingConfiguration, int totalGroupCount) {
        JsonArray results = new JsonArray();
        mapStream.forEach((key, value) -> {
            JsonObject rangeObject = new JsonObject(key);
            JsonObject resultObject = new JsonObject()
                    .put("floor", rangeObject.getLong("floor"))
                    .put("ceil", rangeObject.getLong("ceil"))
                    .put(aggregationFunctionKey, value);

            results.add(resultObject);
        });

        return new JsonObject()
                .put("totalGroupCount", totalGroupCount)
                .put("count", results.size())
                .put("rangeGrouping", new JsonObject()
                        .put("unit", groupingConfiguration.getGroupByUnit())
                        .put("range", groupingConfiguration.getGroupByRange()))
                .put("results", results);
    }

    @SuppressWarnings("unchecked")
    private <T extends Comparable<? super T>> Map<String, T> doNormalSorting(
            Map<String, T> collect, GroupingConfiguration groupingConfiguration) {
        boolean asc = groupingConfiguration.getGroupingSortOrder().equalsIgnoreCase("asc");

        if (collect.isEmpty()) {
            return collect.entrySet().stream()
                    .map(e -> new SimpleEntry<>(e.getKey(), e.getValue()))
                    .collect(toMap(SimpleEntry::getKey, SimpleEntry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        } else {
            final Map.Entry<String, T> next = collect.entrySet().iterator().next();
            boolean isCollection = next.getValue() instanceof Collection || next.getValue() instanceof Map;
            boolean keyIsRanged = groupingConfiguration.hasGroupRanging();

            final Comparator<SimpleEntry<String, T>> comp =
                    keyIsRanged ? Comparator.comparing(e -> new JsonObject(e.getKey()),
                            Comparator.comparing(keyOne -> keyOne.getLong("floor"))) :
                            isCollection ? Comparator.comparing(SimpleEntry::getKey) :
                                    Comparator.comparing(SimpleEntry::getValue);

            final Stream<SimpleEntry<String, T>> sorted = collect.entrySet().stream()
                    .map(e -> new SimpleEntry<>(e.getKey(), e.getValue()))
                    .sorted(asc ? comp : comp.reversed());

            if (groupingConfiguration.isFullList()) {
                return sorted
                        .collect(toMap(SimpleEntry::getKey, SimpleEntry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            } else {
                return sorted
                        .limit(groupingConfiguration.getGroupingListLimit())
                        .collect(toMap(SimpleEntry::getKey, SimpleEntry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends Comparable<? super T>> Map<String, T> doRangedSorting(
            Map<String, T> collect, GroupingConfiguration groupingConfiguration) {
        boolean asc = groupingConfiguration.getGroupingSortOrder().equalsIgnoreCase("asc");
        Comparator<SimpleEntry<String, T>> comp = Comparator.comparingLong(e ->
                new JsonObject(e.getKey()).getLong("floor"));

        final Stream<SimpleEntry<String, T>> sorted = collect.entrySet()
                .stream()
                .map(e -> new SimpleEntry<>(e.getKey(), e.getValue()))
                .sorted(asc ? comp : comp.reversed());

        if (groupingConfiguration.isFullList()) {
            return sorted
                    .collect(toMap(SimpleEntry::getKey, SimpleEntry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        } else {
            return sorted
                    .limit(groupingConfiguration.getGroupingListLimit())
                    .collect(toMap(SimpleEntry::getKey, SimpleEntry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
    }

    private <T> String calculateGroupingKey(T item, GroupingConfiguration groupingConfiguration) {
        String groupingKey;

        try {
            groupingKey = db.getFieldAsString(groupingConfiguration.getGroupBy(), item);
            if (groupingKey == null) throw new UnknownError("Cannot find field!");
        } catch (NullPointerException e) {
            throw new UnknownError("Field is null!");
        }

        if (groupingConfiguration.hasGroupRanging()) {
            String groupByRangeUnit = groupingConfiguration.getGroupByUnit();
            Object groupByRangeRange = groupingConfiguration.getGroupByRange();
            Long groupingValue = null;
            Double rangingValue = null;

            if (groupByRangeUnit.equalsIgnoreCase("INTEGER")) {
                groupingValue = Long.parseLong(groupByRangeRange.toString());
                long value;

                try {
                    value = Long.parseLong(groupingKey);
                } catch (NumberFormatException nfe) {
                    value = (long) Double.parseDouble(groupingKey);
                }

                rangingValue = Math.ceil(value / groupingValue);
            } else if (groupByRangeUnit.equalsIgnoreCase("DATE")) {
                Date date = db.getFieldAsObject(groupingConfiguration.getGroupBy(), item);
                groupingValue = getTimeRangeFromDateUnit(groupByRangeRange.toString());

                rangingValue = Math.ceil(date.getTime() / groupingValue);
            }

            if (rangingValue != null) {
                return new JsonObject()
                        .put("floor", ((long) Math.floor(rangingValue)) * groupingValue)
                        .put("base", groupingValue)
                        .put("ratio", rangingValue)
                        .put("ceil", (((long) Math.ceil(rangingValue)) + 1L) * groupingValue)
                        .encode();
            } else {
                throw new UnknownError("Cannot find field!");
            }
        } else {
            return groupingKey;
        }
    }

    private long getTimeRangeFromDateUnit(String groupByRangeRange) {
        switch (AggregateFunction.TIMEUNIT_DATE.valueOf(groupByRangeRange.toUpperCase())) {
            case HOUR:
                return Duration.ofHours(1).toMillis();
            case TWELVE_HOUR:
                return Duration.ofHours(12).toMillis();
            case DAY:
                return Duration.ofDays(1).toMillis();
            case WEEK:
                return Duration.ofDays(7).toMillis();
            case MONTH:
                return Duration.ofDays(30).toMillis();
            case YEAR:
                return Duration.ofDays(365).toMillis();
            default:
                throw new Error("Invalid TIME UNIT for Date!");
        }
    }

    private void setEtagAndCacheAndReturnContent(String etagKey, int hash, String cacheKey, String content,
                                                 Handler<AsyncResult<String>> resultHandler) {
        String etagItemListHashKey = TYPE.getSimpleName() + "_" + hash + "_" + "itemListEtags";
        String newEtag = ModelUtils.returnNewEtag(content.hashCode());

        cacheManager.replaceAggregationCache(content, () -> cacheKey, cacheRes -> {
            if (cacheRes.failed()) {
                logger.error("Cache failed on agg!");
            }

            if (eTagManager != null) {
                eTagManager.replaceAggregationEtag(etagItemListHashKey, etagKey, newEtag, etagRes -> {
                    if (etagRes.failed()) {
                        resultHandler.handle(Future.failedFuture(etagRes.cause()));
                    } else {
                        resultHandler.handle(Future.succeededFuture(content));
                    }
                });
            } else {
                resultHandler.handle(Future.succeededFuture(content));
            }
        });
    }
}
