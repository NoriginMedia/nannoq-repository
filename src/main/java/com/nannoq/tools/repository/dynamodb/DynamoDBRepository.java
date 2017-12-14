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

package com.nannoq.tools.repository.dynamodb;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.Region;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.nannoq.tools.repository.dynamodb.operators.*;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.DynamoDBModel;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.repository.Repository;
import com.nannoq.tools.repository.repository.cache.CacheManager;
import com.nannoq.tools.repository.repository.cache.ClusterCacheManagerImpl;
import com.nannoq.tools.repository.repository.cache.LocalCacheManagerImpl;
import com.nannoq.tools.repository.repository.etag.ETagManager;
import com.nannoq.tools.repository.repository.etag.InMemoryEtagManagerImpl;
import com.nannoq.tools.repository.repository.etag.RedisETagManagerImpl;
import com.nannoq.tools.repository.repository.redis.RedisUtils;
import com.nannoq.tools.repository.repository.results.ItemListResult;
import com.nannoq.tools.repository.repository.results.ItemResult;
import com.nannoq.tools.repository.services.internal.InternalRepositoryService;
import com.nannoq.tools.repository.utils.*;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;
import org.apache.commons.lang3.ArrayUtils;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * This class defines DynamoDBRepository class. It handles almost all cases of use with the DynamoDB of AWS.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@SuppressWarnings({"Convert2MethodRef", "Duplicates"})
public class DynamoDBRepository<E extends DynamoDBModel & Model & ETagable & Cacheable>
        implements Repository<E>, InternalRepositoryService<E> {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBRepository.class.getSimpleName());

    public static final String PAGINATION_INDEX = "PAGINATION_INDEX";

    protected Vertx vertx;
    private boolean isCached = false;
    private boolean isEtagEnabled = false;

    private final Class<E> TYPE;
    private String HASH_IDENTIFIER;
    private String IDENTIFIER;
    private String PAGINATION_IDENTIFIER;

    private boolean hasRangeKey;
    private static AmazonDynamoDBAsyncClient DYNAMO_DB_CLIENT;
    protected static DynamoDBMapper DYNAMO_DB_MAPPER;
    private RedisClient REDIS_CLIENT;
    private static String S3BucketName;

    private static final Object SYNC_MAPPER_OBJECT = new Object();

    private final DynamoDBParameters<E> parameters;
    private final DynamoDBAggregates<E> aggregates;

    private final DynamoDBCreator<E> creator;
    private final DynamoDBReader<E> reader;
    private final DynamoDBUpdater<E> updater;
    private final DynamoDBDeleter<E> deleter;

    protected CacheManager<E> cacheManager;
    protected ETagManager<E> etagManager;

    private Map<String, Field> fieldMap = new ConcurrentHashMap<>();
    private Map<String, Type> typeMap = new ConcurrentHashMap<>();

    public DynamoDBRepository(Class<E> type, JsonObject appConfig) {
        this(Vertx.currentContext().owner(), type, appConfig, null, null);
    }

    public DynamoDBRepository(Class<E> type, JsonObject appConfig, @Nullable CacheManager<E> cacheManager) {
        this(Vertx.currentContext().owner(), type, appConfig, cacheManager, null);
    }

    public DynamoDBRepository(Class<E> type, JsonObject appConfig, @Nullable ETagManager<E> eTagManager) {
        this(Vertx.currentContext().owner(), type, appConfig, null, eTagManager);
    }

    public DynamoDBRepository(Class<E> type, JsonObject appConfig,
                              @Nullable CacheManager<E> cacheManager, @Nullable ETagManager<E> eTagManager) {
        this(Vertx.currentContext().owner(), type, appConfig, cacheManager, eTagManager);
    }

    public DynamoDBRepository(Vertx vertx, Class<E> type, JsonObject appConfig) {
        this(vertx, type, appConfig, null, null);
    }

    public DynamoDBRepository(Vertx vertx, Class<E> type, JsonObject appConfig,
                              @Nullable CacheManager<E> cacheManager) {
        this(vertx, type, appConfig, cacheManager, null);
    }

    public DynamoDBRepository(Vertx vertx, Class<E> type, JsonObject appConfig,
                              @Nullable ETagManager<E> eTagManager) {
        this(vertx, type, appConfig, null, eTagManager);
    }

    @SuppressWarnings("unchecked")
    public DynamoDBRepository(Vertx vertx, Class<E> type, JsonObject appConfig,
                              @Nullable CacheManager<E> cacheManager, @Nullable ETagManager<E> eTagManager) {
        this.TYPE = type;
        this.vertx = vertx;

        if (Arrays.stream(type.getClass().getAnnotations()).anyMatch(ann -> ann instanceof DynamoDBDocument)) {
            throw new DynamoDBMappingException("This type is a document definition, should not have own repository!");
        }

        synchronized (SYNC_MAPPER_OBJECT) {
            if (DYNAMO_DB_MAPPER == null) setMapper(appConfig);
        }

        Optional<String> tableName = Arrays.stream(TYPE.getDeclaredAnnotations())
                .filter(a -> a instanceof DynamoDBTable)
                .map(a -> (DynamoDBTable) a)
                .map(table -> table.tableName())
                .findFirst();

        String COLLECTION;

        if (tableName.isPresent() || Arrays.stream(TYPE.getDeclaredAnnotations())
                .anyMatch(a -> a instanceof DynamoDBDocument)) {
            COLLECTION = tableName.orElseGet(() ->
                    type.getSimpleName().substring(0, 1).toLowerCase() + type.getSimpleName().substring(1) + "s");

            if (appConfig.getString("redis_host") != null) {
                this.REDIS_CLIENT = RedisUtils.getRedisClient(getVertx(), appConfig);
            }
        } else {
            logger.error("Models must include the DynamoDBTable annotation, with the tablename!");

            throw new IllegalArgumentException("Models must include the DynamoDBTable annotation, with the tablename");
        }

        if (eTagManager != null) {
            this.etagManager = eTagManager;
            isEtagEnabled = true;
        } else {
            if (appConfig.getString("redis_host") != null) {
                this.etagManager = new RedisETagManagerImpl<>(type, getRedisClient());
                isEtagEnabled = true;
            } else {
                this.etagManager = new InMemoryEtagManagerImpl<>(vertx, type);
            }
        }

        if (cacheManager != null) {
            this.cacheManager = cacheManager;
            isCached = true;
        } else if (vertx.isClustered()) {
            this.cacheManager = new ClusterCacheManagerImpl<>(type, vertx);
            isCached = true;
        } else {
            this.cacheManager = new LocalCacheManagerImpl<>(type, vertx);
            isCached = true;
        }

        setHashAndRange(type);
        Map<String, JsonObject> GSI_KEY_MAP = setGsiKeys(type);
        this.cacheManager.initializeCache(res -> isCached = res.succeeded());

        this.parameters = new DynamoDBParameters<>(TYPE, this, HASH_IDENTIFIER, IDENTIFIER, PAGINATION_IDENTIFIER);
        this.aggregates = new DynamoDBAggregates<>(TYPE, this, HASH_IDENTIFIER, IDENTIFIER, this.cacheManager, etagManager);

        this.creator = new DynamoDBCreator<>(TYPE, vertx, this, HASH_IDENTIFIER, IDENTIFIER, this.cacheManager, etagManager);
        this.reader = new DynamoDBReader<>(TYPE, vertx, this, COLLECTION, HASH_IDENTIFIER, IDENTIFIER,
                PAGINATION_IDENTIFIER, GSI_KEY_MAP, parameters, this.cacheManager, this.etagManager);
        this.updater = new DynamoDBUpdater<>(this);
        this.deleter = new DynamoDBDeleter<>(TYPE, vertx, this, HASH_IDENTIFIER, IDENTIFIER, this.cacheManager, etagManager);
    }

    private Vertx getVertx() {
        if (vertx == null) vertx = Vertx.currentContext().owner();

        return vertx;
    }

    public static String getBucketName() {
        return S3BucketName;
    }

    private static void setMapper(JsonObject appConfig) {
        String dynamoDBId = appConfig.getString("dynamo_db_iam_id");
        String dynamoDBKey = appConfig.getString("dynamo_db_iam_key");
        String endPoint = fetchEndPoint(appConfig);

        BasicAWSCredentials creds = new BasicAWSCredentials(dynamoDBId, dynamoDBKey);
        AWSStaticCredentialsProvider statCreds = new AWSStaticCredentialsProvider(creds);

        DYNAMO_DB_CLIENT = new AmazonDynamoDBAsyncClient(statCreds).withEndpoint(endPoint);

//        SecretKey CONTENT_ENCRYPTION_KEY = new SecretKeySpec(
//                DatatypeConverter.parseHexBinary(appConfig.getString("contentEncryptionKeyBase")), "PKCS5Padding");
//
//        SecretKey SIGNING_KEY = new SecretKeySpec(
//                DatatypeConverter.parseHexBinary(appConfig.getString("signingKeyBase")), "HmacSHA256");
//
//        EncryptionMaterialsProvider provider = new SymmetricStaticProvider(CONTENT_ENCRYPTION_KEY, SIGNING_KEY);

        DYNAMO_DB_MAPPER = new DynamoDBMapper(
                DYNAMO_DB_CLIENT, DynamoDBMapperConfig.DEFAULT,
                //new AttributeEncryptor(provider), statCreds);
                statCreds);
    }

    public static DynamoDBMapper getS3DynamoDbMapper() {
        synchronized (SYNC_MAPPER_OBJECT) {
            if (DYNAMO_DB_MAPPER == null) {
                setMapper(Objects.requireNonNull(Vertx.currentContext() == null ?
                        null : Vertx.currentContext().config()));
            }
        }

        return DYNAMO_DB_MAPPER;
    }

    private static String fetchEndPoint(JsonObject appConfig) {
        JsonObject config = appConfig != null ? appConfig :
                (Vertx.currentContext() == null ? null : Vertx.currentContext().config());
        String endPoint;

        if (config == null) {
            endPoint = "http://localhost:8001";
        } else {
            endPoint = config.getString("dynamo_endpoint");
        }

        return endPoint;
    }

    private void setHashAndRange(Class<E> type) {
        Method[] allMethods = getAllMethodsOnType(type);
        HASH_IDENTIFIER = "";
        IDENTIFIER = "";
        PAGINATION_IDENTIFIER = "";

        Arrays.stream(allMethods).filter(method ->
                Arrays.stream(method.getAnnotations())
                        .anyMatch(annotation -> annotation instanceof DynamoDBHashKey))
                .findFirst()
                .ifPresent(method -> HASH_IDENTIFIER = stripGet(method.getName()));

        Arrays.stream(allMethods).filter(method ->
                Arrays.stream(method.getAnnotations())
                        .anyMatch(annotation -> annotation instanceof DynamoDBRangeKey))
                .findFirst()
                .ifPresent(method -> IDENTIFIER = stripGet(method.getName()));

        Arrays.stream(allMethods).filter(method ->
                Arrays.stream(method.getAnnotations())
                        .anyMatch(annotation -> annotation instanceof DynamoDBIndexRangeKey &&
                                ((DynamoDBIndexRangeKey) annotation).localSecondaryIndexName()
                                        .equalsIgnoreCase(PAGINATION_INDEX)))
                .findFirst()
                .ifPresent(method -> PAGINATION_IDENTIFIER = stripGet(method.getName()));

        hasRangeKey = !IDENTIFIER.equals("");
    }

    private static <E> Map<String, JsonObject> setGsiKeys(Class<E> type) {
        Method[] allMethods = getAllMethodsOnType(type);
        Map<String, JsonObject> gsiMap = new ConcurrentHashMap<>();

        Arrays.stream(allMethods).forEach(method -> {
            if (Arrays.stream(method.getDeclaredAnnotations())
                    .anyMatch(annotation -> annotation instanceof DynamoDBIndexHashKey)) {
                final String hashName = method.getDeclaredAnnotation(DynamoDBIndexHashKey.class)
                        .globalSecondaryIndexName();
                final String hash = stripGet(method.getName());
                final String[] range = new String[1];

                if (!hashName.equals("")) {
                    Arrays.stream(allMethods).forEach(rangeMethod -> {
                        if (Arrays.stream(rangeMethod.getDeclaredAnnotations())
                                .anyMatch(annotation -> annotation instanceof DynamoDBIndexRangeKey)) {
                            final String rangeIndexName = rangeMethod.getDeclaredAnnotation(DynamoDBIndexRangeKey.class)
                                    .globalSecondaryIndexName();

                            if (rangeIndexName.equals(hashName)) {
                                range[0] = stripGet(rangeMethod.getName());
                            }
                        }
                    });

                    JsonObject hashKeyObject = new JsonObject()
                            .put("hash", hash);

                    if (range[0] != null) hashKeyObject.put("range", range[0]);

                    gsiMap.put(hashName, hashKeyObject);

                    logger.debug("Detected GSI: " + hashName + " : " + hashKeyObject.encodePrettily());
                }
            }
        });

        return gsiMap;
    }

    private static Method[] getAllMethodsOnType(Class klazz) {
        Method[] methods = klazz.getDeclaredMethods();

        if (klazz.getSuperclass() != null && klazz.getSuperclass() != Object.class) {
            return ArrayUtils.addAll(methods, getAllMethodsOnType(klazz.getSuperclass()));
        }

        return methods;
    }

    public static String stripGet(String string) {
        String newString = string.replace("get", "");
        char c[] = newString.toCharArray();
        c[0] += 32;

        return new String(c);
    }

    @SuppressWarnings("ConstantConditions")
    public Field getField(String fieldName) throws IllegalArgumentException {
        try {
            Field field = fieldMap.get(fieldName);
            if (field != null) return field;

            field = TYPE.getDeclaredField(fieldName);
            if (field != null) fieldMap.put(fieldName, field);
            field.setAccessible(true);

            return field;
        } catch (NoSuchFieldException | NullPointerException e) {
            if (TYPE.getSuperclass() != null && TYPE.getSuperclass() != Object.class) {
                return getField(fieldName, TYPE.getSuperclass());
            }

            throw new UnknownError("Cannot get field " + fieldName + " from " + TYPE.getSimpleName() + "!");
        }
    }

    @SuppressWarnings("ConstantConditions")
    public Field getField(String fieldName, Class klazz) throws IllegalArgumentException {
        try {
            Field field = fieldMap.get(fieldName);
            if (field != null) return field;

            field = klazz.getDeclaredField(fieldName);
            if (field != null) fieldMap.put(fieldName, field);
            field.setAccessible(true);

            return field;
        } catch (NoSuchFieldException | NullPointerException e) {
            if (klazz.getSuperclass() != null && klazz.getSuperclass() != Object.class) {
                return getField(fieldName, klazz.getSuperclass());
            } else {
                logger.error("Cannot get field " + fieldName + " from " + klazz.getSimpleName() + "!", e);
            }

            throw new UnknownError("Cannot find field!");
        }
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public <T, O> T getFieldAsObject(String fieldName, O object) {
        try {
            Field field = fieldMap.get(fieldName);
            if (field != null) return (T) field.get(object);

            field = object.getClass().getDeclaredField(fieldName);
            if (field != null) fieldMap.put(fieldName, field);
            field.setAccessible(true);

            return (T) field.get(object);
        } catch (Exception e) {
            if (object.getClass().getSuperclass() != null && object.getClass().getSuperclass() != Object.class) {
                return getFieldAsObject(fieldName, object, object.getClass().getSuperclass());
            } else {
                logger.error("Cannot get field " + fieldName + " from " + object.getClass().getSimpleName() + "!", e);
            }

            throw new UnknownError("Cannot find field!");
        }
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private <T, O> T getFieldAsObject(String fieldName, O object, Class klazz) {
        try {
            Field field = fieldMap.get(fieldName);
            if (field != null) return (T) field.get(object);

            field = object.getClass().getDeclaredField(fieldName);
            if (field != null) fieldMap.put(fieldName, field);
            field.setAccessible(true);

            return (T) field.get(object);
        } catch (Exception e) {
            if (klazz.getSuperclass() != null && klazz.getSuperclass() != Object.class) {
                return getFieldAsObject(fieldName, object, klazz.getSuperclass());
            } else {
                logger.error("Cannot get field " + fieldName + " from " + klazz.getSimpleName() + "!", e);
            }

            throw new UnknownError("Cannot find field!");
        }
    }

    @SuppressWarnings("ConstantConditions")
    public <T> String getFieldAsString(String fieldName, T object) {
        if (logger.isTraceEnabled()) { logger.trace("Getting " + fieldName + " from " + object.getClass().getSimpleName()); }

        try {
            Field field = fieldMap.get(fieldName);

            if (field != null) {
                field.setAccessible(true);

                return field.get(object).toString();
            }

            field = TYPE.getDeclaredField(fieldName);
            if (field != null) fieldMap.put(fieldName, field);
            field.setAccessible(true);

            Object fieldObject = field.get(object);

            return fieldObject.toString();
        } catch (Exception e) {
            if (TYPE.getSuperclass() != null && TYPE.getSuperclass() != Object.class) {
                return getFieldAsString(fieldName, object, TYPE.getSuperclass());
            } else {
                logger.error("Cannot get " + fieldName + " as string from: " + Json.encodePrettily(object), e);

                throw new UnknownError("Cannot find field!");
            }
        }
    }

    @SuppressWarnings("ConstantConditions")
    private <T> String getFieldAsString(String fieldName, T object, Class klazz) {
        if (logger.isTraceEnabled()) { logger.trace("Getting " + fieldName + " from " + klazz.getSimpleName()); }

        try {
            Field field = fieldMap.get(fieldName);

            if (field != null) {
                field.setAccessible(true);

                return field.get(object).toString();
            }

            field = klazz.getDeclaredField(fieldName);
            if (field != null) fieldMap.put(fieldName, field);
            field.setAccessible(true);

            Object fieldObject = field.get(object);

            return fieldObject.toString();
        } catch (Exception e) {
            if (klazz.getSuperclass() != null && klazz.getSuperclass() != Object.class) {
                return getFieldAsString(fieldName, object, klazz.getSuperclass());
            } else {
                logger.error("Cannot get " + fieldName + " as string from: " + Json.encodePrettily(object) + ", klazzwise!", e);

                throw new UnknownError("Cannot find field!");
            }
        }
    }

    @SuppressWarnings("ConstantConditions")
    public Field checkAndGetField(String fieldName) throws IllegalArgumentException {
        try {
            Field field = fieldMap.get(fieldName);

            if (field == null) {
                field = TYPE.getDeclaredField(fieldName);

                if (field != null) fieldMap.put(fieldName, field);
            }

            Type fieldType = typeMap.get(fieldName);

            if (fieldType == null) {
                fieldType = field.getType();

                if (fieldType != null) typeMap.put(fieldName, fieldType);
            }

            if (fieldType == Long.class || fieldType == Integer.class ||
                    fieldType == Double.class || fieldType == Float.class ||
                    fieldType == Short.class ||
                    fieldType == long.class || fieldType == int.class ||
                    fieldType == double.class || fieldType == float.class ||
                    fieldType == short.class) {
                field.setAccessible(true);

                return field;
            } else {
                throw new IllegalArgumentException("Not an incrementable field!");
            }
        } catch (NoSuchFieldException | NullPointerException e) {
            if (TYPE.getSuperclass() != null && TYPE.getSuperclass() != Object.class) {
                return checkAndGetField(fieldName, TYPE.getSuperclass());
            } else {
                throw new IllegalArgumentException("Field does not exist!");
            }
        }
    }

    @SuppressWarnings("ConstantConditions")
    private Field checkAndGetField(String fieldName, Class klazz) throws IllegalArgumentException {
        try {
            Field field = fieldMap.get(fieldName);

            if (field == null) {
                field = klazz.getDeclaredField(fieldName);

                if (field != null) fieldMap.put(fieldName, field);
            }

            Type fieldType = typeMap.get(fieldName);

            if (fieldType == null) {
                fieldType = field.getType();

                if (fieldType != null) typeMap.put(fieldName, fieldType);
            }

            if (fieldType == Long.class || fieldType == Integer.class ||
                    fieldType == Double.class || fieldType == Float.class ||
                    fieldType == Short.class ||
                    fieldType == long.class || fieldType == int.class ||
                    fieldType == double.class || fieldType == float.class ||
                    fieldType == short.class) {
                field.setAccessible(true);

                return field;
            } else {
                throw new IllegalArgumentException("Not an incrementable field!");
            }
        } catch (NoSuchFieldException e) {
            if (klazz.getSuperclass() != null && klazz.getSuperclass() != Object.class) {
                return checkAndGetField(fieldName, klazz);
            } else {
                throw new IllegalArgumentException("Field does not exist!");
            }
        }
    }

    public boolean hasField(Field[] fields, String key) {
        boolean hasField = Arrays.stream(fields).anyMatch(field -> field.getName().equalsIgnoreCase(key));

        return hasField || hasField(TYPE.getSuperclass(), key);
    }

    private boolean hasField(Class klazz, String key) {
        try {
            Field field = fieldMap.get(key);

            if (field == null) {
                field = klazz.getDeclaredField(key);

                if (field != null) fieldMap.put(key, field);
            }

            boolean hasField = field != null;

            return hasField || hasField(klazz.getSuperclass(), key);
        } catch (NoSuchFieldException | NullPointerException e) {
            return false;
        }
    }

    private static Type extractFieldType(Class type, String fieldName) {
        try {
            return type.getDeclaredField(fieldName).getType();
        } catch (NoSuchFieldException e) {
            if (type.getSuperclass() != null && type.getSuperclass() != Object.class) {
                return extractFieldType(type.getSuperclass(), fieldName);
            }

            throw new UnknownError("Cannot find field!");
        }
    }

    public String getAlternativeIndexIdentifier(String indexName) {
        final String[] identifier = new String[1];

        Arrays.stream(TYPE.getMethods()).filter(method ->
                Arrays.stream(method.getAnnotations())
                        .anyMatch(annotation -> annotation instanceof DynamoDBIndexRangeKey &&
                                ((DynamoDBIndexRangeKey) annotation).localSecondaryIndexName()
                                        .equalsIgnoreCase(indexName)))
                .findFirst()
                .ifPresent(method -> identifier[0] = stripGet(method.getName()));

        return identifier[0];
    }

    @SuppressWarnings("ConstantConditions")
    public <T> AttributeValue getIndexValue(String alternateIndex, T object) {
        try {
            Field field = fieldMap.get(alternateIndex);

            if (field == null) {
                field = object.getClass().getDeclaredField(alternateIndex);

                if (field != null) fieldMap.put(alternateIndex, field);
            }

            field.setAccessible(true);

            Type fieldType = typeMap.get(alternateIndex);

            if (fieldType == null) {
                fieldType = field.getType();

                if (fieldType != null) typeMap.put(alternateIndex, fieldType);
            }

            if (fieldType == Date.class) {
                Date dateObject = (Date) field.get(object);

                return createAttributeValue(alternateIndex, String.valueOf(dateObject.getTime()));
            } else {
                return createAttributeValue(alternateIndex, String.valueOf(field.get(object)));
            }
        } catch (NoSuchFieldException | NullPointerException | IllegalAccessException e) {
            if (object.getClass().getSuperclass() != null && object.getClass().getSuperclass() != Object.class) {
                return getIndexValue(alternateIndex, object, object.getClass().getSuperclass());
            }
        }

        throw new UnknownError("Cannot find field!");
    }

    @SuppressWarnings("ConstantConditions")
    private <T> AttributeValue getIndexValue(String alternateIndex, T object, Class klazz) {
        try {
            Field field = fieldMap.get(alternateIndex);

            if (field == null) {
                field = klazz.getDeclaredField(alternateIndex);

                if (field != null) fieldMap.put(alternateIndex, field);
            }

            field.setAccessible(true);

            Type fieldType = typeMap.get(alternateIndex);

            if (fieldType == null) {
                fieldType = field.getType();

                if (fieldType != null) typeMap.put(alternateIndex, fieldType);
            }

            if (fieldType == Date.class) {
                Date dateObject = (Date) field.get(object);

                return createAttributeValue(alternateIndex, String.valueOf(dateObject.getTime()));
            } else {
                return createAttributeValue(alternateIndex, String.valueOf(field.get(object)));
            }
        } catch (NoSuchFieldException | NullPointerException | IllegalAccessException e) {
            if (klazz.getSuperclass() != null && klazz.getSuperclass() != Object.class) {
                return getIndexValue(alternateIndex, object, klazz.getSuperclass());
            }
        }

        throw new UnknownError("Cannot find field!");
    }

    public AttributeValue createAttributeValue(String fieldName, String valueAsString) {
        return createAttributeValue(fieldName, valueAsString, null);
    }

    public AttributeValue createAttributeValue(String fieldName, String valueAsString, ComparisonOperator modifier) {
        Field field = getField(fieldName);
        Type fieldType = typeMap.get(fieldName);

        if (fieldType == null) {
            fieldType = field.getType();

            if (fieldType != null) typeMap.put(fieldName, fieldType);
        }

        if (fieldType == String.class) {
            return new AttributeValue().withS(valueAsString);
        } else if (fieldType == Integer.class || fieldType == Double.class || fieldType == Long.class) {
            try {
                if (fieldType == Integer.class) {
                    int value = Integer.parseInt(valueAsString);

                    if (modifier == ComparisonOperator.GE) value -= 1;
                    if (modifier == ComparisonOperator.LE) value += 1;

                    return new AttributeValue().withN(String.valueOf(value));
                }

                if (fieldType == Double.class) {
                    double value = Double.parseDouble(valueAsString);

                    if (modifier == ComparisonOperator.GE) value -= 0.1;
                    if (modifier == ComparisonOperator.LE) value += 0.1;

                    return new AttributeValue().withN(String.valueOf(value));
                }

                if (fieldType == Long.class) {
                    long value = Long.parseLong(valueAsString);

                    if (modifier == ComparisonOperator.GE) value -= 1;
                    if (modifier == ComparisonOperator.LE) value += 1;

                    return new AttributeValue().withN(String.valueOf(value));
                }
            } catch (NumberFormatException nfe) {
                logger.error("Cannot recreate attribute!", nfe);
            }

            return new AttributeValue().withN(valueAsString);
        } else if (fieldType == Boolean.class) {
            if (valueAsString.equalsIgnoreCase("true")) {
                return new AttributeValue().withN("1");
            } else if (valueAsString.equalsIgnoreCase("false")) {
                return new AttributeValue().withN("0");
            }

            try {
                int boolValue = Integer.parseInt(valueAsString);

                if (boolValue == 1 || boolValue == 0) {
                    return new AttributeValue().withN(String.valueOf(boolValue));
                }

                throw new UnknownError("Cannot create AttributeValue!");
            } catch (NumberFormatException nfe) {
                logger.error("Cannot rceate attribute!", nfe);
            }
        } else if (fieldType == Date.class) {
            try {
                if (logger.isDebugEnabled()) { logger.debug("Date received: " + valueAsString); }

                Date date;

                try {
                    date = new Date(Long.parseLong(valueAsString));
                } catch (NumberFormatException nfe) {
                    DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
                    date = df1.parse(valueAsString);
                }

                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);

                if (modifier == ComparisonOperator.LE) calendar.add(Calendar.MILLISECOND, 1);
                if (modifier == ComparisonOperator.GE) calendar.setTime(new Date(calendar.getTime().getTime() - 1));

                DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
                df2.setTimeZone(TimeZone.getTimeZone("Z"));
                if (logger.isDebugEnabled()) { logger.debug("DATE IS: " + df2.format(calendar.getTime())); }

                return new AttributeValue().withS(df2.format(calendar.getTime()));
            } catch (ParseException e) {
                return new AttributeValue().withS(valueAsString);
            }
        } else {
            return new AttributeValue().withS(valueAsString);
        }

        throw new UnknownError("Cannot create attributevalue!");
    }

    public E fetchNewestRecord(Class<E> type, String hash, String range) {
        if (range != null && hasRangeKey) {
            if (logger.isDebugEnabled()) {
                logger.debug("Loading newest with range!");
            }

            return DYNAMO_DB_MAPPER.load(TYPE, hash, range);
        } else {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Loading newest by hash query!");
                }

                DynamoDBQueryExpression<E> query =
                        new DynamoDBQueryExpression<>();
                E keyObject = type.newInstance();
                keyObject.setHash(hash);
                query.setConsistentRead(true);
                query.setHashKeyValues(keyObject);
                query.setLimit(1);

                long timeBefore = System.currentTimeMillis();

                List<E> items = DYNAMO_DB_MAPPER.query(TYPE, query);

                if (logger.isDebugEnabled()) {
                    logger.debug("Results received in: " + (System.currentTimeMillis() - timeBefore) + " ms");
                }

                if (!items.isEmpty()) {
                    return items.get(0);
                } else {
                    return null;
                }
            } catch (Exception e) {
                logger.error("Error fetching newest!", e);

                return null;
            }
        }
    }

    public ExpectedAttributeValue buildExpectedAttributeValue(String value, boolean exists) {
        ExpectedAttributeValue exp = new ExpectedAttributeValue(exists);
        if (exp.isExists()) exp.setValue(new AttributeValue().withS(value));

        return exp;
    }

    @Override
    public Function<E, E> incrementField(E record, String fieldName) throws IllegalArgumentException {
        return (r) -> {
            updater.incrementField(record, fieldName);

            return record;
        };
    }

    @Override
    public Function<E, E> decrementField(E record, String fieldName) throws IllegalArgumentException {
        return (r) -> {
            updater.decrementField(record, fieldName);

            return record;
        };
    }

    @Override
    public void read(JsonObject identifiers, Handler<AsyncResult<ItemResult<E>>> asyncResultHandler) {
        reader.read(identifiers, asyncResultHandler);
    }

    @Override
    public void read(JsonObject identifiers, boolean consistent, String[] projections, Handler<AsyncResult<ItemResult<E>>> asyncResultHandler) {
        reader.read(identifiers, consistent, projections, asyncResultHandler);
    }

    @Override
    public void readAll(Handler<AsyncResult<List<E>>> asyncResultHandler) {
        reader.readAll(asyncResultHandler);
    }

    @Override
    public void readAll(JsonObject identifiers, Map<String, List<FilterParameter>> filterParameterMap, Handler<AsyncResult<List<E>>> asyncResultHandler) {
        reader.readAll(identifiers, filterParameterMap, asyncResultHandler);
    }

    @Override
    public void readAll(JsonObject identifiers, String pageToken, QueryPack queryPack, String[] projections, Handler<AsyncResult<ItemListResult<E>>> asyncResultHandler) {
        reader.readAll(identifiers, pageToken, queryPack, projections, asyncResultHandler);
    }

    @Override
    public void readAll(String pageToken, QueryPack queryPack, String[] projections, Handler<AsyncResult<ItemListResult<E>>> asyncResultHandler) {
        reader.readAll(pageToken, queryPack, projections, asyncResultHandler);
    }

    public void readAll(JsonObject identifiers, String pageToken, QueryPack queryPack, String[] projections, String GSI, Handler<AsyncResult<ItemListResult<E>>> asyncResultHandler) {
        reader.readAll(identifiers, pageToken, queryPack, projections, GSI, asyncResultHandler);
    }

    @Override
    public void aggregation(JsonObject identifiers, QueryPack queryPack, String[] projections, Handler<AsyncResult<String>> resultHandler) {
        aggregation(identifiers, queryPack, projections, null, resultHandler);
    }

    public void aggregation(JsonObject identifiers, QueryPack queryPack, String[] projections, String GSI, Handler<AsyncResult<String>> resultHandler) {
        aggregates.aggregation(identifiers, queryPack, projections, GSI, resultHandler);
    }

    @Override
    public JsonObject buildParameters(Map<String, List<String>> queryMap,
                                      Field[] fields, Method[] methods, JsonObject errors,
                                      Map<String, List<FilterParameter>> params, int[] limit,
                                      Queue<OrderByParameter> orderByQueue, String[] indexName) {
        return parameters.buildParameters(queryMap, fields, methods, errors, params, limit, orderByQueue, indexName);
    }

    @Override
    public void readAllWithoutPagination(String identifier, Handler<AsyncResult<List<E>>> asyncResultHandler) {
        reader.readAllWithoutPagination(identifier, asyncResultHandler);
    }

    @Override
    public void readAllWithoutPagination(String identifier, QueryPack queryPack, Handler<AsyncResult<List<E>>> asyncResultHandler) {
        reader.readAllWithoutPagination(identifier, queryPack, asyncResultHandler);
    }

    @Override
    public void readAllWithoutPagination(String identifier, QueryPack queryPack, String[] projections, Handler<AsyncResult<List<E>>> asyncResultHandler) {
        readAllWithoutPagination(identifier, queryPack, projections, null, asyncResultHandler);
    }

    public void readAllWithoutPagination(String identifier, QueryPack queryPack, String[] projections, String GSI,
                                         Handler<AsyncResult<List<E>>> asyncResultHandler) {
        reader.readAllWithoutPagination(identifier, queryPack, projections, GSI, asyncResultHandler);
    }

    @Override
    public void readAllWithoutPagination(QueryPack queryPack, String[] projections, Handler<AsyncResult<List<E>>> asyncResultHandler) {
        readAllWithoutPagination(queryPack, projections, null, asyncResultHandler);
    }

    public void readAllWithoutPagination(QueryPack queryPack, String[] projections, String GSI, Handler<AsyncResult<List<E>>> asyncResultHandler) {
        reader.readAllWithoutPagination(queryPack, projections, GSI, asyncResultHandler);
    }

    public void readAllPaginated(Handler<AsyncResult<PaginatedParallelScanList<E>>> resultHandler) {
        reader.readAllPaginated(resultHandler);
    }

    @Override
    public void doWrite(boolean create, Map<E, Function<E, E>> records, Handler<AsyncResult<List<E>>> asyncResultHandler) {
        creator.doWrite(create, records, asyncResultHandler);
    }

    @Override
    public void doDelete(List<JsonObject> identifiers, Handler<AsyncResult<List<E>>> asyncResultHandler) {
        deleter.doDelete(identifiers, asyncResultHandler);
    }

    @Override
    public InternalRepositoryService<E> remoteCreate(E record, Handler<AsyncResult<E>> asyncResultHandler) {
        create(record, res -> {
            if (res.failed()) {
                asyncResultHandler.handle(Future.failedFuture(res.cause()));
            } else {
                asyncResultHandler.handle(Future.succeededFuture(res.result().getItem()));
            }
        });

        return this;
    }

    @Override
    public InternalRepositoryService<E> remoteRead(JsonObject identifiers, Handler<AsyncResult<E>> asyncResultHandler) {
        read(identifiers, res -> {
            if (res.failed()) {
                asyncResultHandler.handle(Future.failedFuture(res.cause()));
            } else {
                asyncResultHandler.handle(res.map(res.result().getItem()));
            }
        });

        return this;
    }

    @Override
    public InternalRepositoryService<E> remoteIndex(JsonObject identifier, Handler<AsyncResult<List<E>>> asyncResultHandler) {
        readAllWithoutPagination(identifier.getString("hash"), asyncResultHandler);

        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public InternalRepositoryService<E> remoteUpdate(E record, Handler<AsyncResult<E>> asyncResultHandler) {
        update(record, r -> (E) record.setModifiables(r), res -> {
            if (res.failed()) {
                asyncResultHandler.handle(Future.failedFuture(res.cause()));
            } else {
                asyncResultHandler.handle(Future.succeededFuture(res.result().getItem()));
            }
        });

        return this;
    }

    @Override
    public InternalRepositoryService<E> remoteDelete(JsonObject identifiers, Handler<AsyncResult<E>> asyncResultHandler) {
        delete(identifiers, res -> {
            if (res.failed()) {
                asyncResultHandler.handle(Future.failedFuture(res.cause()));
            } else {
                asyncResultHandler.handle(Future.succeededFuture(res.result().getItem()));
            }
        });

        return this;
    }

    protected String getModelName() {
        return TYPE.getSimpleName();
    }

    public static Future<Void> initializeDynamoDb(JsonObject appConfig, Map<String, Class> collectionMap) {
        Future<Void> future = Future.future();

        initializeDynamoDb(appConfig, collectionMap, future.completer());

        return future;
    }

    public static void initializeDynamoDb(JsonObject appConfig, Map<String, Class> collectionMap,
                                          Handler<AsyncResult<Void>> resultHandler) {
        if (logger.isDebugEnabled()) { logger.debug("Initializing DynamoDB"); }

        try {
            setMapper(appConfig);
            silenceDynamoDBLoggers();
            List<Future> futures = new ArrayList<>();

            collectionMap.forEach((k, v) -> futures.add(initialize(DYNAMO_DB_CLIENT, DYNAMO_DB_MAPPER, k, v)));

            CompositeFuture.all(futures).setHandler(res -> {
                if (logger.isDebugEnabled()) { logger.debug("Preparing S3 Bucket"); }

                S3BucketName = appConfig.getString("content_bucket");

                SimpleModule s3LinkModule = new SimpleModule("MyModule", new Version(1, 0, 0, null));
                s3LinkModule.addSerializer(new S3LinkSerializer());
                s3LinkModule.addDeserializer(S3Link.class, new S3LinkDeserializer(appConfig));

                Json.mapper.registerModule(s3LinkModule);

                if (logger.isDebugEnabled()) { logger.debug("DynamoDB Ready"); }

                if (res.failed()) {
                    resultHandler.handle(Future.failedFuture(res.cause()));
                } else {
                    resultHandler.handle(Future.succeededFuture());
                }
            });
        } catch (Exception e) {
            logger.error("Unable to initialize!", e);
        }
    }

    private static void silenceDynamoDBLoggers() {
        java.util.logging.Logger.getLogger("com.amazonaws").setLevel(java.util.logging.Level.WARNING);
    }

    private static Future<Void> initialize(AmazonDynamoDBAsyncClient client, DynamoDBMapper mapper,
                                              String COLLECTION, Class TYPE) {
        Future<Void> future = Future.future();

        initialize(client, mapper, COLLECTION, TYPE, future.completer());

        return future;
    }

    private static void initialize(AmazonDynamoDBAsyncClient client, DynamoDBMapper mapper,
                                   String COLLECTION, Class TYPE,
                                   Handler<AsyncResult<Void>> resultHandler) {
        client.listTablesAsync(new AsyncHandler<ListTablesRequest, ListTablesResult>() {
            private final Long DEFAULT_WRITE_TABLE = 100L;
            private final Long DEFAULT_READ_TABLE = 100L;
            private final Long DEFAULT_WRITE_GSI = 100L;
            private final Long DEFAULT_READ_GSI = 100L;

            @Override
            public void onError(Exception e) {
                logger.error("Cannot use this repository for creation, no connection: " + e);

                resultHandler.handle(Future.failedFuture(e));
            }

            @Override
            public void onSuccess(ListTablesRequest request, ListTablesResult listTablesResult) {
                boolean tableExists = listTablesResult.getTableNames().contains(COLLECTION);

                if (logger.isDebugEnabled()) {
                    logger.debug("Table is available: " + tableExists);
                }

                if (tableExists) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Table exists for: " + COLLECTION + ", doing nothing...");
                    }

                    resultHandler.handle(Future.succeededFuture());
                } else {
                    CreateTableRequest req = mapper.generateCreateTableRequest(TYPE)
                            .withProvisionedThroughput(new ProvisionedThroughput()
                                    .withWriteCapacityUnits(DEFAULT_WRITE_TABLE)
                                    .withReadCapacityUnits(DEFAULT_READ_TABLE));

                    final Projection allProjection = new Projection().withProjectionType(ProjectionType.ALL);
                    req.setLocalSecondaryIndexes(req.getLocalSecondaryIndexes().stream()
                            .peek(lsi -> lsi.setProjection(allProjection))
                            .collect(toList()));
                    setAnyGlobalSecondaryIndexes(req, DEFAULT_READ_GSI, DEFAULT_WRITE_GSI);

                    client.createTableAsync(req, new AsyncHandler<CreateTableRequest, CreateTableResult>() {
                        @Override
                        public void onError(Exception e) {
                            logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));
                            if (logger.isDebugEnabled()) {
                                logger.debug("Could not remoteCreate table for: " + COLLECTION);
                            }

                            resultHandler.handle(Future.failedFuture(e));
                        }

                        @Override
                        public void onSuccess(CreateTableRequest request, CreateTableResult createTableResult) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Table creation for: " + COLLECTION + " is success: " +
                                        createTableResult.getTableDescription()
                                                .getTableName()

                                                .equals(COLLECTION));
                            }

                            waitForTableAvailable(createTableResult, res -> {
                                if (res.failed()) {
                                    resultHandler.handle(Future.failedFuture(res.cause()));
                                } else {
                                    resultHandler.handle(Future.succeededFuture());
                                }
                            });
                        }
                    });
                }
            }

            @SuppressWarnings("SameParameterValue")
            private void setAnyGlobalSecondaryIndexes(CreateTableRequest req,
                                                      long readProvisioning, long writeProvisioning) {
                @SuppressWarnings("unchecked")
                final Map<String, JsonObject> map = setGsiKeys(TYPE);

                if (map.size() > 0) {
                    List<GlobalSecondaryIndex> gsis = new ArrayList<>();

                    map.forEach((k, v) ->
                            gsis.add(new GlobalSecondaryIndex()
                                    .withIndexName(k)
                                    .withProjection(new Projection()
                                            .withProjectionType(ProjectionType.ALL))
                                    .withKeySchema(new KeySchemaElement()
                                            .withKeyType(KeyType.HASH)
                                            .withAttributeName(v.getString("hash")),
                                                    new KeySchemaElement()
                                            .withKeyType(KeyType.RANGE)
                                            .withAttributeName(v.getString("range")))
                                    .withProvisionedThroughput(new ProvisionedThroughput()
                                            .withReadCapacityUnits(readProvisioning)
                                            .withWriteCapacityUnits(writeProvisioning))));

                    req.withGlobalSecondaryIndexes(gsis);
                }
            }

            private void waitForTableAvailable(CreateTableResult createTableResult,
                                               Handler<AsyncResult<Void>> resultHandler) {
                final String tableName = createTableResult.getTableDescription().getTableName();

                final DescribeTableResult describeTableResult = client.describeTable(tableName);

                if (tableReady(describeTableResult)) {
                    logger.debug(tableName + " created and active: " + Json.encodePrettily(
                            describeTableResult.getTable()));

                    resultHandler.handle(Future.succeededFuture());
                } else {
                    Handler<AsyncResult<Void>> resHandle = res -> {
                        if (res.failed()) {
                            waitForTableAvailable(createTableResult, resultHandler);
                        } else {
                            resultHandler.handle(Future.succeededFuture());
                        }
                    };

                    waitForActive(tableName, resHandle);
                }
            }

            private boolean tableReady(DescribeTableResult describeTableResult) {
                return describeTableResult.getTable().getTableStatus().equalsIgnoreCase("ACTIVE") &&
                        describeTableResult.getTable().getGlobalSecondaryIndexes().stream()
                                .allMatch(i -> i.getIndexStatus().equals("ACTIVE"));
            }

            private void waitForActive(String tableName, Handler<AsyncResult<Void>> resHandle) {
                if (client.describeTable(tableName).getTable().getTableStatus().equals("ACTIVE")) {
                    resHandle.handle(Future.succeededFuture());
                } else {
                    resHandle.handle(Future.failedFuture("Not active!"));
                }
            }
        });
    }

    public static S3Link createS3Link(DynamoDBMapper dynamoDBMapper, String path) {
        return dynamoDBMapper.createS3Link(Region.EU_Ireland, S3BucketName, path);
    }

    public static String createSignedUrl(DynamoDBMapper dynamoDBMapper, S3Link file) {
        return createSignedUrl(dynamoDBMapper, 7, file);
    }

    @SuppressWarnings("SameParameterValue")
    public static String createSignedUrl(DynamoDBMapper dynamoDBMapper, int days, S3Link file) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, days);

        GeneratePresignedUrlRequest signReq = new GeneratePresignedUrlRequest(file.getBucketName(), file.getKey());
        signReq.setMethod(com.amazonaws.HttpMethod.GET);
        signReq.setExpiration(calendar.getTime());

        URL url = dynamoDBMapper.getS3ClientCache().getClient(Region.EU_Ireland).generatePresignedUrl(signReq);

        return url.toString();
    }

    protected String[] buildEventbusProjections(JsonArray projectionArray) {
        if (projectionArray == null) return new String[] {};

        List<String> projections = projectionArray.stream()
                .map(Object::toString)
                .collect(toList());

        String[] projectionArrayStrings = new String[projections == null ? 0 : projections.size()];

        if (projections != null) {
            IntStream.range(0, projections.size()).forEach(i -> projectionArrayStrings[i] = projections.get(i));
        }

        return projectionArrayStrings;
    }

    public boolean hasRangeKey() {
        return hasRangeKey;
    }

    public DynamoDBMapper getDynamoDbMapper() {
        return DYNAMO_DB_MAPPER;
    }

    public RedisClient getRedisClient() {
        return REDIS_CLIENT;
    }
}
