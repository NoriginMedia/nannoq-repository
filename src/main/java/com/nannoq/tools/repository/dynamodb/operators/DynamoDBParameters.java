package com.nannoq.tools.repository.dynamodb.operators;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.DynamoDBModel;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.utils.FilterParameter;
import com.nannoq.tools.repository.utils.OrderByParameter;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.nannoq.tools.repository.dynamodb.DynamoDBRepository.PAGINATION_INDEX;
import static com.nannoq.tools.repository.repository.Repository.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * This class is used to define the parameters for various operations for the DynamoDBRepository.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class DynamoDBParameters<E extends DynamoDBModel & Model & ETagable & Cacheable> {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBParameters.class.getSimpleName());

    private final Class<E> TYPE;
    private final DynamoDBRepository<E> db;

    private final String HASH_IDENTIFIER;
    private final String IDENTIFIER;
    private final String PAGINATION_IDENTIFIER;

    public DynamoDBParameters(Class<E> type, DynamoDBRepository<E> db,
                              String HASH_IDENTIFIER, String IDENTIFIER, String PAGINATION_IDENTIFIER) {
        TYPE = type;
        this.db = db;
        this.HASH_IDENTIFIER = HASH_IDENTIFIER;
        this.IDENTIFIER = IDENTIFIER;
        this.PAGINATION_IDENTIFIER = PAGINATION_IDENTIFIER;
    }

    public JsonObject buildParameters(Map<String, List<String>> queryMap,
                                      Field[] fields, Method[] methods,
                                      JsonObject errors,
                                      Map<String, List<FilterParameter<E>>> params, int[] limit,
                                      Queue<OrderByParameter> orderByQueue, String[] indexName) {
        queryMap.keySet().forEach(key -> {
            if (key.equalsIgnoreCase(LIMIT_KEY) || key.equalsIgnoreCase(MULTIPLE_IDS_KEY) ||
                    key.equalsIgnoreCase(ORDER_BY_KEY) || key.equalsIgnoreCase(PROJECTION_KEY) ||
                    db.hasField(fields, key)) {
                List<String> values = queryMap.get(key);

                if (key.equalsIgnoreCase(PROJECTION_KEY) || key.equalsIgnoreCase(MULTIPLE_IDS_KEY)) {
                    if (logger.isDebugEnabled()) { logger.debug("Ignoring Projection Key..."); }
                } else {
                    if (values == null) {
                        errors.put("field_null", "Value in '" + key + "' cannot be null!");
                    } else if (key.equalsIgnoreCase(ORDER_BY_KEY)) {
                        JsonArray jsonArray;

                        try {
                            jsonArray = new JsonArray(values.get(0));
                        } catch (Exception arrayParseException) {
                            jsonArray = new JsonArray();
                            jsonArray.add(new JsonObject(values.get(0)));
                        }

                        final int[] orderByParamCount = {0};

                        if (jsonArray.size() == 1) {
                            jsonArray.stream().forEach(orderByParam -> {
                                OrderByParameter orderByParameter =
                                        Json.decodeValue(orderByParam.toString(), OrderByParameter.class);
                                if (orderByParameter.isValid()) {
                                    Arrays.stream(methods).forEach(method -> {
                                        DynamoDBIndexRangeKey indexRangeKey =
                                                method.getDeclaredAnnotation(DynamoDBIndexRangeKey.class);

                                        if (indexRangeKey != null &&
                                                db.stripGet(method.getName()).equals(orderByParameter.getField())) {
                                            indexName[0] = indexRangeKey.localSecondaryIndexName();
                                            orderByQueue.add(orderByParameter);
                                        }
                                    });

                                    if (indexName[0] == null || orderByQueue.isEmpty()) {
                                        errors.put("orderBy_parameter_" + orderByParamCount[0] + "_error",
                                                "This is not a valid remoteIndex!");
                                    }
                                } else {
                                    errors.put("orderBy_parameter_" + orderByParamCount[0] + "_error",
                                            "Field cannot be null!");
                                }

                                orderByParamCount[0]++;
                            });
                        } else {
                            errors.put("orderBy_limit_error", "You must and may only order by a single remoteIndex!");
                        }
                    } else if (key.equalsIgnoreCase(LIMIT_KEY)) {
                        try {
                            if (logger.isDebugEnabled()) { logger.debug("Parsing limit.."); }

                            limit[0] = Integer.parseInt(values.get(0));

                            if (logger.isDebugEnabled()) { logger.debug("Limit is: " + limit[0]); }

                            if (limit[0] < 1) {
                                errors.put(key + "_negative_error", "Limit must be a whole positive Integer!");
                            } else if (limit[0] > 100) {
                                errors.put(key + "_exceed_max_error", "Maximum limit is 100!");
                            }
                        } catch (NumberFormatException nfe) {
                            errors.put(key + "_error", "Limit must be a whole positive Integer!");
                        }
                    } else {
                        try {
                            db.parseParam(TYPE, values.get(0), key, params, errors);
                        } catch (Exception e) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Could not parse filterParams as a JsonObject, attempting array...", e);
                            }

                            try {
                                JsonArray jsonArray = new JsonArray(values.get(0));

                                jsonArray.forEach(jsonObjectAsString ->
                                        db.parseParam(TYPE, jsonObjectAsString.toString(), key, params, errors));
                            } catch (Exception arrayException) {
                                logger.error("Unable to parse json as array:" + values.get(0));

                                errors.put(key + "_value_json_error", "Unable to parse this json...");
                            }
                        }
                    }
                }
            } else {
                errors.put(key + "_field_error", "This field does not exist on the selected resource.");
            }
        });

        return errors;
    }

    @SuppressWarnings("Duplicates")
    DynamoDBQueryExpression<E> applyParameters(OrderByParameter peek,
                                               Map<String, List<FilterParameter<E>>> params) {
        final DynamoDBQueryExpression<E> filterExpression = new DynamoDBQueryExpression<>();

        if (params != null) {
            final String[] keyConditionString = {""};
            Map<String, String> ean = new HashMap<>();
            Map<String, AttributeValue> eav = new HashMap<>();
            final int[] paramCount = {0};
            final int[] count = {0};
            final int paramSize = params.keySet().size();

            params.keySet().stream().map(params::get).forEach(paramList -> {
                final int[] orderCounter = {0};
                keyConditionString[0] += "(";

                if (paramList.size() > 1 && isRangeKey(peek, paramList.get(0)) &&
                        !isIllegalRangedKeyQueryParams(paramList)) {
                    buildMultipleRangeKeyCondition(filterExpression, params.size(), paramList,
                            peek != null ? peek.getField() : paramList.get(0).getField());
                } else {
                    paramList.forEach(param -> {
                        final String fieldName = param.getField().charAt(param.getField().length() - 2) == '_' ?
                                param.getField().substring(0, param.getField().length() - 2) : param.getField();
                        ean.put("#name" + count[0], fieldName);

                        if (param.isEq()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.EQ, param.getType(),
                                        db.createAttributeValue(fieldName, param.getEq().toString()));
                            } else {
                                int curCount = count[0]++;
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getEq().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                        curCount + " = :val" + curCount;
                            }
                        } else if (param.isNe()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.NE, param.getType(),
                                        db.createAttributeValue(fieldName, param.getEq().toString()));
                            } else {
                                int curCount = count[0]++;
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getNe().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                        curCount + " <> :val" + curCount;
                            }
                        } else if (param.isBetween()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.BETWEEN, param.getType(),
                                        db.createAttributeValue(fieldName, param.getGt().toString()),
                                        db.createAttributeValue(fieldName, param.getLt().toString()));
                            } else {
                                int curCount = count[0];
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getGt().toString()));
                                eav.put(":val" + (curCount + 1), db.createAttributeValue(fieldName, param.getLt().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                        curCount + " > :val" + curCount + " and " +
                                        " #name" + curCount + " < " + " :val" + (curCount + 1);

                                count[0] = curCount + 2;
                            }
                        } else if (param.isGt()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.GT, param.getType(),
                                        db.createAttributeValue(fieldName, param.getGt().toString()));
                            } else {
                                int curCount = count[0]++;
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getGt().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                        curCount + " > :val" + curCount;
                            }
                        } else if (param.isLt()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.LT, param.getType(),
                                        db.createAttributeValue(fieldName, param.getLt().toString()));
                            } else {
                                int curCount = count[0]++;
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getLt().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                        curCount + " < :val" + curCount;
                            }
                        } else if (param.isInclusiveBetween()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.BETWEEN, param.getType(),
                                        db.createAttributeValue(fieldName, param.getGe().toString(), ComparisonOperator.GE),
                                        db.createAttributeValue(fieldName, param.getLe().toString(), ComparisonOperator.LE));
                            } else {
                                int curCount = count[0];
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getGe().toString()));
                                eav.put(":val" + (curCount + 1), db.createAttributeValue(fieldName, param.getLe().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                        curCount + " >= :val" + curCount + " and " +
                                        " #name" + curCount + " =< " + " :val" + (curCount + 1);

                                count[0] = curCount + 2;
                            }
                        } else if (param.isGeLtVariableBetween()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.BETWEEN, param.getType(),
                                        db.createAttributeValue(fieldName, param.getGe().toString(), ComparisonOperator.GE),
                                        db.createAttributeValue(fieldName, param.getLt().toString()));
                            } else {
                                int curCount = count[0];
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getGe().toString()));
                                eav.put(":val" + (curCount + 1), db.createAttributeValue(fieldName, param.getLt().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                        curCount + " >= :val" + curCount + " and " +
                                        " #name" + curCount + " < " + " :val" + (curCount + 1);

                                count[0] = curCount + 2;
                            }
                        } else if (param.isLeGtVariableBetween()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.BETWEEN, param.getType(),
                                        db.createAttributeValue(fieldName, param.getGt().toString()),
                                        db.createAttributeValue(fieldName, param.getLe().toString(), ComparisonOperator.LE));
                            } else {
                                int curCount = count[0];
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getLe().toString()));
                                eav.put(":val" + (curCount + 1), db.createAttributeValue(fieldName, param.getGt().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                        curCount + " <= :val" + curCount + " and " +
                                        " #name" + curCount + " > " + " :val" + (curCount + 1);

                                count[0] = curCount + 2;
                            }
                        } else if (param.isGe()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.GE, param.getType(),
                                        db.createAttributeValue(fieldName, param.getGe().toString()));
                            } else {
                                int curCount = count[0]++;
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getGe().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                        curCount + " >= :val" + curCount;
                            }
                        } else if (param.isLe()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.LE, param.getType(),
                                        db.createAttributeValue(fieldName, param.getLe().toString()));
                            } else {
                                int curCount = count[0]++;
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getLe().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                        curCount + " <= :val" + curCount;
                            }
                        } else if (param.isContains()) {
                            int curCount = count[0]++;
                            eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getContains().toString()));

                            keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") +
                                    "contains(" + "#name" + curCount + ", :val" + curCount + ")";
                        } else if (param.isNotContains()) {
                            int curCount = count[0]++;
                            eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getNotContains().toString()));

                            keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") +
                                    "not contains(" + "#name" + curCount + ", :val" + curCount + ")";
                        } else if (param.isBeginsWith()) {
                            if (isRangeKey(peek, fieldName, param)) {
                                buildRangeKeyCondition(filterExpression, params.size(),
                                        peek != null ? peek.getField() : param.getField(),
                                        ComparisonOperator.BEGINS_WITH, param.getType(),
                                        db.createAttributeValue(fieldName, param.getBeginsWith().toString()));
                            } else {
                                int curCount = count[0]++;
                                eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getBeginsWith().toString()));

                                keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") +
                                        "begins_with(" + "#name" + curCount + ", :val" + curCount + ")";
                            }
                        } else if (param.isIn()) {
                            AttributeValue inList = inQueryToStringChain(fieldName, param.getIn());

                            Queue<String> keys = new ConcurrentLinkedQueue<>();
                            AtomicInteger valCounter = new AtomicInteger();
                            inList.getL().forEach(av -> {
                                String currentValKey = ":inVal" + valCounter.getAndIncrement();
                                keys.add(currentValKey);
                                eav.put(currentValKey, av);
                            });

                            int curCount = count[0]++;

                            keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                    curCount + " IN (" + keys.stream().collect(joining(", ")) + ")";
                        }

                        orderCounter[0]++;
                    });

                    if (keyConditionString[0].endsWith("(")) {
                        if (keyConditionString[0].equalsIgnoreCase("(")) {
                            keyConditionString[0] = "";
                        } else {
                            keyConditionString[0] =
                                    keyConditionString[0].substring(0, keyConditionString[0].lastIndexOf(")"));
                        }
                    } else {
                        keyConditionString[0] += ")";

                        if (paramSize > 1 && paramCount[0] < paramSize - 1) {
                            keyConditionString[0] += " AND ";
                        }
                    }

                    paramCount[0]++;
                }
            });

            if (ean.size() > 0 && eav.size() > 0) {
                filterExpression.withFilterExpression(keyConditionString[0])
                        .withExpressionAttributeNames(ean)
                        .withExpressionAttributeValues(eav);
            }

            if (logger.isDebugEnabled()) { logger.debug("RANGE KEY EXPRESSION IS: " + filterExpression.getRangeKeyConditions()); }
            if (logger.isDebugEnabled()) { logger.debug("FILTER EXPRESSION IS: " + filterExpression.getFilterExpression()); }
            if (logger.isDebugEnabled()) { logger.debug("NAMES: " + Json.encodePrettily(ean)); }
            if (logger.isDebugEnabled()) { logger.debug("Values: " + Json.encodePrettily(eav)); }
        }

        return filterExpression;
    }

    private AttributeValue inQueryToStringChain(String fieldName, Object[] in) {
        return new AttributeValue().withL(Arrays.stream(in)
                .map(o -> db.createAttributeValue(fieldName, o.toString()))
                .collect(toList()));
    }

    private boolean isRangeKey(OrderByParameter peek, FilterParameter<E> param) {
        return isRangeKey(peek, param.getField().charAt(param.getField().length() - 2) == '_' ?
                param.getField().substring(0, param.getField().length() - 2) : param.getField(), param);
    }

    private boolean isRangeKey(OrderByParameter peek, String fieldName, FilterParameter<E> param) {
        return (peek != null && peek.getField().equalsIgnoreCase(fieldName)) ||
                (peek == null && param.getField().equalsIgnoreCase(PAGINATION_IDENTIFIER));
    }

    @SuppressWarnings("Duplicates")
    DynamoDBScanExpression applyParameters(Map<String, List<FilterParameter<E>>> params) {
        final DynamoDBScanExpression filterExpression = new DynamoDBScanExpression();

        if (params != null) {
            final String[] keyConditionString = {""};
            Map<String, String> ean = new HashMap<>();
            Map<String, AttributeValue> eav = new HashMap<>();
            final int[] paramCount = {0};
            final int[] count = {0};
            final int paramSize = params.keySet().size();

            params.keySet().stream().map(params::get).forEach(paramList -> {
                keyConditionString[0] += "(";
                final int[] orderCounter = {0};

                paramList.forEach(param -> {
                    final String fieldName = param.getField().charAt(param.getField().length() - 2) == '_' ?
                            param.getField().substring(0, param.getField().length() - 2) : param.getField();
                    ean.put("#name" + count[0], fieldName);

                    if (param.isEq()) {
                        int curCount = count[0]++;
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getEq().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " = :val" + curCount;
                    } else if (param.isNe()) {
                        int curCount = count[0]++;
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getNe().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " <> :val" + curCount;
                    } else if (param.isBetween()) {
                        int curCount = count[0];
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getGt().toString()));
                        eav.put(":val" + (curCount + 1), db.createAttributeValue(fieldName, param.getLt().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " > :val" + curCount + " and " +
                                " #name" + curCount + " < " + " :val" + (curCount + 1);

                        count[0] = curCount + 2;
                    } else if (param.isGt()) {
                        int curCount = count[0]++;
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getGt().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " > :val" + curCount;
                    } else if (param.isLt()) {
                        int curCount = count[0]++;
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getLt().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " < :val" + curCount;
                    } else if (param.isInclusiveBetween()) {
                        int curCount = count[0];
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getGe().toString()));
                        eav.put(":val" + (curCount + 1), db.createAttributeValue(fieldName, param.getLe().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " >= :val" + curCount + " and " +
                                " #name" + curCount + " =< " + " :val" + (curCount + 1);

                        count[0] = curCount + 2;
                    } else if (param.isGeLtVariableBetween()) {
                        int curCount = count[0];
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getGe().toString()));
                        eav.put(":val" + (curCount + 1), db.createAttributeValue(fieldName, param.getLt().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " >= :val" + curCount + " and " +
                                " #name" + curCount + " < " + " :val" + (curCount + 1);

                        count[0] = curCount + 2;
                    } else if (param.isLeGtVariableBetween()) {
                        int curCount = count[0];
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getLe().toString()));
                        eav.put(":val" + (curCount + 1), db.createAttributeValue(fieldName, param.getGt().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " <= :val" + curCount + " and " +
                                " #name" + curCount + " > " + " :val" + (curCount + 1);

                        count[0] = curCount + 2;
                    } else if (param.isGe()) {
                        int curCount = count[0]++;
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getGe().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " >= :val" + curCount;
                    } else if (param.isLe()) {
                        int curCount = count[0]++;
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getLe().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " <= :val" + curCount;
                    } else if (param.isContains()) {
                        int curCount = count[0]++;
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getContains().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") +
                                "contains(" + "#name" + curCount + ", :val" + curCount + ")";
                    } else if (param.isNotContains()) {
                        int curCount = count[0]++;
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getNotContains().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") +
                                "not contains(" + "#name" + curCount + ", :val" + curCount + ")";
                    } else if (param.isBeginsWith()) {
                        int curCount = count[0]++;
                        eav.put(":val" + curCount, db.createAttributeValue(fieldName, param.getBeginsWith().toString()));

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") +
                                "begins_with(" + "#name" + curCount + ", :val" + curCount + ")";
                    } else if (param.isIn()) {
                        AttributeValue inList = inQueryToStringChain(fieldName, param.getIn());

                        Queue<String> keys = new ConcurrentLinkedQueue<>();
                        AtomicInteger valCounter = new AtomicInteger();
                        inList.getL().forEach(av -> {
                            String currentValKey = ":inVal" + valCounter.getAndIncrement();
                            keys.add(currentValKey);
                            eav.put(currentValKey, av);
                        });

                        int curCount = count[0]++;

                        keyConditionString[0] += (orderCounter[0] == 0 ? "" : " " + param.getType() + " ") + "#name" +
                                curCount + " IN (" + keys.stream().collect(joining(", ")) + ")";
                    }

                    orderCounter[0]++;
                });

                keyConditionString[0] += ")";

                if (paramSize > 1 && paramCount[0] < paramSize - 1) {
                    keyConditionString[0] += " AND ";
                }

                paramCount[0]++;
            });

            if (ean.size() > 0 && eav.size() > 0) {
                filterExpression.withFilterExpression(keyConditionString[0])
                        .withExpressionAttributeNames(ean)
                        .withExpressionAttributeValues(eav);
            }

            if (logger.isDebugEnabled()) { logger.debug("SCAN EXPRESSION IS: " + filterExpression.getFilterExpression()); }
            if (logger.isDebugEnabled()) { logger.debug("NAMES: " + Json.encodePrettily(ean)); }
            if (logger.isDebugEnabled()) { logger.debug("Values: " + Json.encodePrettily(eav)); }
        }

        return filterExpression;
    }

    private void buildRangeKeyConditionForMultipleIds(DynamoDBQueryExpression<E> filterExpression,
                                                      String field, List<String> ids) {
        List<AttributeValue> idsAsAttributeValues = ids.stream()
                .map(id -> new AttributeValue().withS(id))
                .collect(toList());

        filterExpression.withRangeKeyCondition(field, new Condition()
                .withComparisonOperator(ComparisonOperator.IN)
                .withAttributeValueList(idsAsAttributeValues));
    }

    private void buildRangeKeyCondition(DynamoDBQueryExpression<E> filterExpression, int count,
                                        String field, ComparisonOperator comparator, String type,
                                        AttributeValue... attributeValues) {
        filterExpression.withRangeKeyCondition(field, new Condition()
                .withComparisonOperator(comparator)
                .withAttributeValueList(attributeValues));

        if (count > 1) filterExpression.withConditionalOperator(type);
    }

    private void buildMultipleRangeKeyCondition(DynamoDBQueryExpression<E> filterExpression, int count,
                                                List<FilterParameter<E>> paramList,
                                                String rangeKeyName) {
        if (paramList.size() > 2) throw new IllegalArgumentException("Cannot query on more than two params on a range key!");

        FilterParameter<E> paramOne = paramList.get(0);
        FilterParameter<E> paramTwo = paramList.get(1);
        Condition condition = new Condition();

        if (paramOne.isGt() && paramTwo.isLt()) {
            condition.withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(
                            db.createAttributeValue(paramOne.getField(), paramOne.getGt().toString()),
                            db.createAttributeValue(paramTwo.getField(), paramTwo.getLt().toString()));
        } else if (paramOne.isLt() && paramTwo.isGt()) {
            condition.withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(
                            db.createAttributeValue(paramTwo.getField(), paramTwo.getGt().toString()),
                            db.createAttributeValue(paramOne.getField(), paramOne.getLt().toString()));
        } else if (paramOne.isLe() && paramTwo.isGe()) {
            condition.withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(
                            db.createAttributeValue(paramTwo.getField(), paramTwo.getGe().toString()),
                            db.createAttributeValue(paramOne.getField(), paramOne.getLe().toString()));
        } else if (paramOne.isGe() && paramTwo.isLe()) {
            condition.withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(
                            db.createAttributeValue(paramOne.getField(), paramOne.getGe().toString()),
                            db.createAttributeValue(paramTwo.getField(), paramTwo.getLe().toString()));
        } else if (paramOne.isGe() && paramTwo.isLt()) {
            condition.withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(
                            db.createAttributeValue(paramOne.getField(), paramOne.getGe().toString()),
                            db.createAttributeValue(paramTwo.getField(), paramTwo.getLt().toString()));
        } else if (paramOne.isLt() && paramTwo.isGe()) {
            condition.withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(
                            db.createAttributeValue(paramTwo.getField(), paramTwo.getGe().toString()),
                            db.createAttributeValue(paramOne.getField(), paramOne.getLt().toString()));
        } else if (paramOne.isLe() && paramTwo.isGt()) {
            condition.withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(
                            db.createAttributeValue(paramTwo.getField(), paramTwo.getGt().toString()),
                            db.createAttributeValue(paramOne.getField(), paramOne.getLe().toString()));
        } else if (paramOne.isGt() && paramTwo.isLe()) {
            condition.withComparisonOperator(ComparisonOperator.BETWEEN)
                    .withAttributeValueList(
                            db.createAttributeValue(paramOne.getField(), paramOne.getGt().toString()),
                            db.createAttributeValue(paramTwo.getField(), paramTwo.getLe().toString()));
        } else {
            throw new IllegalArgumentException("This is an invalid query!");
        }

        filterExpression.withRangeKeyCondition(rangeKeyName, condition);

        if (count > 1) filterExpression.withConditionalOperator("AND");
    }

    DynamoDBQueryExpression<E> applyOrderBy(Queue<OrderByParameter> orderByQueue, String GSI, String indexName,
                                            DynamoDBQueryExpression<E> filterExpression) {
        if (orderByQueue == null || orderByQueue.size() == 0) {
            if (GSI != null) {
                filterExpression.setIndexName(GSI);
            } else {
                filterExpression.setIndexName(getPaginationIndex());
            }

            filterExpression.setScanIndexForward(false);
        } else {
            orderByQueue.forEach(orderByParameter -> {
                if (GSI != null) {
                    filterExpression.setIndexName(GSI);
                } else {
                    filterExpression.setIndexName(indexName);
                }

                filterExpression.setScanIndexForward(orderByParameter.isAsc());
            });
        }

        return filterExpression;
    }

    boolean isIllegalRangedKeyQueryParams(List<FilterParameter<E>> nameParams) {
        return nameParams.stream()
                .anyMatch(FilterParameter::isIllegalRangedKeyParam);
    }

    String[] buildProjections(String[] projections, String indexName) {
        if (projections == null || projections.length == 0) return projections;
        String[] finalProjections = projections;

        if (Arrays.stream(finalProjections).noneMatch(p -> p.equalsIgnoreCase(HASH_IDENTIFIER))) {
            String[] newProjectionArray = new String[finalProjections.length + 1];
            IntStream.range(0, finalProjections.length).forEach(i -> newProjectionArray[i] = finalProjections[i]);
            newProjectionArray[finalProjections.length] = HASH_IDENTIFIER;
            projections = newProjectionArray;
        }

        String[] finalProjections2 = projections;

        if (db.hasRangeKey()) {
            if (Arrays.stream(finalProjections2).noneMatch(p -> p.equalsIgnoreCase(IDENTIFIER))) {
                String[] newProjectionArray = new String[finalProjections2.length + 1];
                IntStream.range(0, finalProjections2.length).forEach(i -> newProjectionArray[i] = finalProjections2[i]);
                newProjectionArray[finalProjections2.length] = IDENTIFIER;
                projections = newProjectionArray;
            }
        }

        String[] finalProjections3 = projections;

        if (Arrays.stream(finalProjections3).noneMatch(p -> p.equalsIgnoreCase(indexName))) {
            String[] newProjectionArray = new String[finalProjections3.length + 1];
            IntStream.range(0, finalProjections3.length).forEach(i -> newProjectionArray[i] = finalProjections3[i]);
            newProjectionArray[finalProjections3.length] = indexName;
            projections = newProjectionArray;
        }

        return projections;
    }

    Map<String, AttributeValue> createPageTokenMap(String encodedPageToken,
                                                   String hashIdentifier,
                                                   String rangeIdentifier,
                                                   String paginationIdentifier,
                                                   String GSI,
                                                   Map<String, JsonObject> GSI_KEY_MAP) {
        Map<String, AttributeValue> pageTokenMap = null;
        String pageToken = null;

        if (encodedPageToken != null) {
            pageToken = new String(Base64.getUrlDecoder().decode(encodedPageToken));
        }

        if (pageToken != null && pageToken.contains("/")) {
            pageTokenMap = new HashMap<>();
            String[] pageTokenArray = pageToken.split("/");
            AttributeValue hashValue = new AttributeValue().withS(pageTokenArray[0]);
            pageTokenMap.putIfAbsent(hashIdentifier, hashValue);

            if (rangeIdentifier != null && !rangeIdentifier.equals("")) {
                AttributeValue rangeValue = new AttributeValue().withS(pageTokenArray[1]);
                pageTokenMap.putIfAbsent(rangeIdentifier, rangeValue);
            }

            if (paginationIdentifier != null && !paginationIdentifier.equals("")) {
                AttributeValue pageValue = db.createAttributeValue(paginationIdentifier, pageTokenArray[2]);
                pageTokenMap.putIfAbsent(paginationIdentifier, pageValue);
            }

            if (GSI != null) {
                final JsonObject keyObject = GSI_KEY_MAP.get(GSI);
                final String hash = keyObject.getString("hash");
                final String range = keyObject.getString("range");

                AttributeValue gsiHash = new AttributeValue().withS(pageTokenArray[3]);
                pageTokenMap.putIfAbsent(hash, gsiHash);

                if (range != null && pageTokenArray.length > 4) {
                    AttributeValue gsiRange = new AttributeValue().withS(pageTokenArray[4]);
                    pageTokenMap.putIfAbsent(range, gsiRange);
                }
            }
        } else if (pageToken != null) {
            AttributeValue hashValue = new AttributeValue().withS(pageToken);
            pageTokenMap = new HashMap<>();
            pageTokenMap.putIfAbsent(hashIdentifier, hashValue);
        }

        return pageTokenMap;
    }

    String createNewPageToken(String hashIdentifier, String identifier, String index,
                              Map<String, AttributeValue> lastEvaluatedKey, String GSI,
                              Map<String, JsonObject> GSI_KEY_MAP, String alternateIndex) {
        if (logger.isDebugEnabled()) { logger.debug("Last key is: " + lastEvaluatedKey); }

        Object indexValue;

        if (index == null && alternateIndex == null) {
            indexValue = null;
        } else {
            indexValue = extractIndexValue(
                    lastEvaluatedKey.get(alternateIndex == null ? index : alternateIndex));
        }

        if (logger.isDebugEnabled()) { logger.debug("Index value is: " + indexValue); }

        final AttributeValue identifierValue = lastEvaluatedKey.get(identifier);

        String newPageToken = lastEvaluatedKey.get(hashIdentifier).getS() +
                (identifierValue == null ? "" : "/" + identifierValue.getS()) + (indexValue == null ? "" :
                "/" + indexValue);

        if (GSI != null) {
            final JsonObject keyObject = GSI_KEY_MAP.get(GSI);
            final String hash = keyObject.getString("hash");
            final String range = keyObject.getString("range");

            newPageToken += "/" + lastEvaluatedKey.get(hash).getS();

            if (range != null && lastEvaluatedKey.get(range) != null) {
                newPageToken += "/" + lastEvaluatedKey.get(range).getS();
            }
        }

        return Base64.getUrlEncoder().encodeToString(newPageToken.getBytes());
    }

    private Object extractIndexValue(AttributeValue attributeValue) {
        if (attributeValue == null) return null;

        if (attributeValue.getS() != null) {
            return attributeValue.getS();
        } else if (attributeValue.getBOOL() != null) {
            return attributeValue.getBOOL();
        } else if (attributeValue.getN() != null) {
            return attributeValue.getN();
        }

        throw new UnknownError("Cannot find indexvalue!");
    }

    private String getPaginationIndex() {
        return PAGINATION_IDENTIFIER != null && !PAGINATION_IDENTIFIER.equals("") ? PAGINATION_INDEX : null;
    }
}
