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

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.S3Link;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.hazelcast.config.Config;
import com.nannoq.tools.repository.dynamodb.model.TestModel;
import com.nannoq.tools.repository.dynamodb.model.TestModelDynamoDBRepository;
import com.nannoq.tools.repository.dynamodb.service.TestModelInternalService;
import com.nannoq.tools.repository.repository.results.CreateResult;
import com.nannoq.tools.repository.repository.results.ItemListResult;
import com.nannoq.tools.repository.repository.results.ItemResult;
import com.nannoq.tools.repository.utils.*;
import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.serviceproxy.ProxyHelper;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import redis.embedded.RedisServer;

import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.nannoq.tools.repository.dynamodb.DynamoDBRepository.PAGINATION_INDEX;
import static com.nannoq.tools.repository.utils.AggregateFunctions.MAX;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

/**
 * This class defines DynamoDBRepository class. It handles almost all cases of use with the DynamoDB of AWS.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@RunWith(VertxUnitRunner.class)
public class DynamoDBRepositoryTestIT {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBRepositoryTestIT.class.getSimpleName());

    private final JsonObject config = new JsonObject()
            .put("redis_host", System.getProperty("redis.endpoint"))
            .put("redis_port", Integer.parseInt(System.getProperty("redis.port")))
            .put("dynamo_endpoint", System.getProperty("dynamo.endpoint"))
            .put("dynamo_db_iam_id", "someTestId")
            .put("dynamo_db_iam_key", "someTestKey")
            .put("content_bucket", "someName");

    private final Date testDate = new Date();
    private final Supplier<TestModel> nonNullTestModel = () -> new TestModel()
            .setSomeStringOne("testString")
            .setSomeStringTwo("testStringRange")
            .setSomeStringThree("testStringThree")
            .setSomeLong(1L)
            .setSomeDate(testDate)
            .setSomeDateTwo(new Date());

    private static Vertx vertx;
    private RedisServer redisServer;
    private TestModelDynamoDBRepository repo;
    private MessageConsumer<JsonObject> serviceRegistration;
    private TestModelInternalService service;
    private final String tableName = TestModel.class.getAnnotation(DynamoDBTable.class).tableName();
    private final Map<String, Class> testMap = Collections.singletonMap(tableName, TestModel.class);

    @Rule public TestName name = new TestName();

    @SuppressWarnings("Duplicates")
    @BeforeClass
    public static void setUpClass(TestContext testContext) {
        Async async = testContext.async();

        Config hzConfig = new Config() ;
        hzConfig.setProperty("hazelcast.logging.type", "log4j2");
        HazelcastClusterManager mgr = new HazelcastClusterManager();
        mgr.setConfig(hzConfig);
        VertxOptions options = new VertxOptions().setClusterManager(mgr);

        Vertx.clusteredVertx(options, clustered -> {
            if (clustered.failed()) {
                logger.error("Vertx not able to cluster!");

                System.exit(-1);
            } else {
                vertx = clustered.result();

                logger.info("Vertx is Running!");
            }

            async.complete();
        });
    }

    @Before
    public void setUp(TestContext testContext) throws Exception {
        logger.info("Running " + name.getMethodName());

        Async async = testContext.async();

        redisServer = new RedisServer(Integer.parseInt(System.getProperty("redis.port")));
        redisServer.start();

        DynamoDBRepository.initializeDynamoDb(config, testMap, res -> {
            if (res.failed()) {
                testContext.fail(res.cause());
            } else {
                try {
                    repo = new TestModelDynamoDBRepository(vertx, config);
                    serviceRegistration = ProxyHelper.registerService(TestModelInternalService.class, vertx, repo, "REPO");
                    service = ProxyHelper.createProxy(TestModelInternalService.class, vertx, "REPO");
                } catch (Exception e) {
                    testContext.fail(e);
                }
            }

            long redisChecker = System.currentTimeMillis();

            while (!redisServer.isActive()) {
                if (System.currentTimeMillis() > redisChecker + 10000) {
                    logger.error("No connection with Redis, terminating!");

                    System.exit(-1);
                }
            }

            logger.info("Sleep for DynamoDBLocal table creation...");

            vertx.setTimer(2000L, aLong -> {
                logger.info("Ready!");

                async.complete();
            });
        });
    }

    @After
    public void tearDown(TestContext testContext) {
        Async async = testContext.async();

        final AmazonDynamoDBAsync amazonDynamoDBAsyncClient = AmazonDynamoDBAsyncClient.asyncBuilder()
                .withEndpointConfiguration(new EndpointConfiguration(config.getString("dynamo_endpoint"), "eu-west-1"))
                .build();
        amazonDynamoDBAsyncClient.deleteTable(tableName);

        repo = null;
        redisServer.stop();
        redisServer = null;
        serviceRegistration.unregister(unregRes -> {
            service = null;
            async.complete();
        });

        logger.info("Closing " + name.getMethodName());
    }

    @SuppressWarnings("Duplicates")
    private void createXItems(int count, Handler<AsyncResult<List<CreateResult<TestModel>>>> resultHandler) {
        final List<TestModel> items = new ArrayList<>();
        List<Future> futures = new CopyOnWriteArrayList<>();

        IntStream.range(0, count).forEach(i -> {
            TestModel testModel = nonNullTestModel.get().setRange(UUID.randomUUID().toString());

            LocalDate startDate = LocalDate.of(1990, 1, 1);
            LocalDate endDate = LocalDate.now();
            long start = startDate.toEpochDay();
            long end = endDate.toEpochDay();

            @SuppressWarnings("ConstantConditions")
            long randomEpochDay = ThreadLocalRandom.current().longs(start, end).findAny().getAsLong();

            testModel.setSomeDate(new Date(randomEpochDay + 1000L));
            testModel.setSomeDateTwo(new Date(randomEpochDay));
            testModel.setSomeLong(new Random().nextLong());

            items.add(testModel);
        });

        items.forEach(item -> {
            Future<CreateResult<TestModel>> future = Future.future();

            repo.create(item, future.completer());

            futures.add(future);

            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {}
        });

        CompositeFuture.all(futures).setHandler(res -> {
            if (res.failed()) {
                resultHandler.handle(Future.failedFuture(res.cause()));
            } else {
                @SuppressWarnings("unchecked")
                final List<CreateResult<TestModel>> collect = futures.stream()
                        .map(Future::result)
                        .map(o -> (CreateResult<TestModel>) o)
                        .collect(toList());

                resultHandler.handle(Future.succeededFuture(collect));
            }
        });
    }

    @SuppressWarnings("Duplicates")
    @AfterClass
    public static void tearDownClass(TestContext testContext) {
        Async async = testContext.async();

        vertx.close(res -> {
            if (res.failed()) {
                logger.error("Vertx failed close!", res.cause());

                vertx.close();
            } else {
                logger.info("Vertx is closed!");
            }

            async.complete();
        });
    }

    @Test
    public void getBucketName() {
        assertEquals("BucketName does not match Config!", config.getString("content_bucket"), repo.getBucketName());
    }

    @Test
    public void stripGet() {
        assertEquals("stripGet does not strip correctly!", "someStringOne", DynamoDBRepository.stripGet("getSomeStringOne"));
    }

    @Test
    public void getField() {
        assertNotNull("Field is null!", repo.getField("someStringOne"));
    }

    @Test(expected = UnknownError.class)
    public void getFieldFail() {
        repo.getField("someBogusField");
    }

    @Test
    public void getFieldAsObject() {
        assertNotNull("FieldAsObject is null!", repo.getFieldAsObject("someStringOne", nonNullTestModel.get()));
    }

    @Test
    public void getFieldAsString() {
        assertNotNull("FieldAsString is null!", repo.getField("someStringOne"));
        assertEquals(repo.getFieldAsString("someStringOne", nonNullTestModel.get()).getClass(), String.class);
    }

    @Test
    public void checkAndGetField() {
        assertNotNull("CheckAndGetField is null!", repo.checkAndGetField("someLong"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkAndGetFieldNonIncrementable() {
        assertNotNull("CheckAndGetField is null!", repo.checkAndGetField("someStringOne"));
    }

    @Test
    public void hasField() {
        final Field[] declaredFields = TestModel.class.getDeclaredFields();

        assertTrue(repo.hasField(declaredFields, "someStringOne"));
        assertFalse(repo.hasField(declaredFields, "someBogusField"));
    }

    @Test
    public void getAlternativeIndexIdentifier() {
        assertEquals("alternateIndex not correct!", "someDate", repo.getAlternativeIndexIdentifier(PAGINATION_INDEX));
    }

    @Test
    public void getIndexValue() throws ParseException {
        AttributeValue attributeValue = repo.getIndexValue("someDate", nonNullTestModel.get());
        DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        Date date = df1.parse(attributeValue.getS());

        assertEquals("Not the same date!", testDate, date);
    }

    @Test
    public void createAttributeValue() {
        final AttributeValue attributeValueString = repo.createAttributeValue("someStringOne", "someTestString");

        assertEquals("Value not correct!", "someTestString", attributeValueString.getS());

        final AttributeValue attributeValueLong = repo.createAttributeValue("someLong", "1000");

        assertEquals("Value not correct!", "1000", attributeValueLong.getN());
    }

    @Test
    public void createAttributeValueWithComparison() {
        final AttributeValue attributeValueGE = repo.createAttributeValue("someLong", "1000", ComparisonOperator.GE);
        final AttributeValue attributeValueLE = repo.createAttributeValue("someLong", "1000", ComparisonOperator.LE);

        assertEquals("Value not correct!", "999", attributeValueGE.getN());
        assertEquals("Value not correct!", "1001", attributeValueLE.getN());
    }

    @Test
    public void fetchNewestRecord(TestContext testContext) {
        Async async = testContext.async();

        repo.create(nonNullTestModel.get()).setHandler(res -> {
            final TestModel testModel = res.result().getItem();
            final TestModel newest = repo.fetchNewestRecord(TestModel.class, testModel.getHash(), testModel.getRange());

            testContext.assertNotNull(newest);
            testContext.assertTrue(Objects.equals(testModel, newest),
                    "Original: " + testModel.toJsonFormat().encodePrettily() +
                            ", Newest: " + newest.toJsonFormat().encodePrettily());

            async.complete();
        });
    }

    @Test
    public void buildExpectedAttributeValue() {
        final ExpectedAttributeValue exists = repo.buildExpectedAttributeValue("someStringOne", true);
        final ExpectedAttributeValue notExists = repo.buildExpectedAttributeValue("someStringOne", false);

        assertTrue(exists.isExists());
        assertFalse(notExists.isExists());

        assertEquals("someStringOne", exists.getValue().getS());
        assertNull(notExists.getValue());
    }

    @Test
    public void incrementField(TestContext testContext) {
        Async async = testContext.async();

        repo.create(nonNullTestModel.get()).setHandler(res -> {
            TestModel item = res.result().getItem();
            long count = item.getSomeLong();

            repo.update(item, repo.incrementField(item, "someLong"), updateRes -> {
                if (updateRes.failed()) {
                    testContext.fail(updateRes.cause());
                } else {
                    TestModel updatedItem = updateRes.result().getItem();

                    testContext.assertNotEquals(count, updatedItem.getSomeLong());
                    testContext.assertTrue(updatedItem.getSomeLong() == count + 1);
                }

                async.complete();
            });
        });
    }

    @Test
    public void decrementField(TestContext testContext) {
        Async async = testContext.async();

        repo.create(nonNullTestModel.get()).setHandler(res -> {
            TestModel item = res.result().getItem();
            long count = item.getSomeLong();

            repo.update(item, repo.decrementField(item, "someLong"), updateRes -> {
                if (updateRes.failed()) {
                    testContext.fail(updateRes.cause());
                } else {
                    TestModel updatedItem = updateRes.result().getItem();

                    testContext.assertNotEquals(count, updatedItem.getSomeLong());
                    testContext.assertTrue(updatedItem.getSomeLong() == count - 1);
                }

                async.complete();
            });
        });
    }

    @Test
    public void create(TestContext testContext) {
        Async async = testContext.async();

        repo.create(nonNullTestModel.get(), createRes -> {
            testContext.assertTrue(createRes.succeeded());
            testContext.assertEquals(createRes.result().getItem().getHash(), nonNullTestModel.get().getHash());
            testContext.assertEquals(createRes.result().getItem().getRange(), nonNullTestModel.get().getRange());

            async.complete();
        });
    }

    @Test
    public void update(TestContext testContext) {
        Async async = testContext.async();

        repo.create(nonNullTestModel.get(), createRes -> {
            testContext.assertTrue(createRes.succeeded());
            testContext.assertEquals(createRes.result().getItem().getHash(), nonNullTestModel.get().getHash());
            testContext.assertEquals(createRes.result().getItem().getRange(), nonNullTestModel.get().getRange());

            Date testDate = new Date();
            repo.update(createRes.result().getItem(), tm -> tm.setSomeDateTwo(testDate), updateResultAsyncResult -> {
                testContext.assertTrue(updateResultAsyncResult.succeeded());
                testContext.assertNotEquals(updateResultAsyncResult.result().getItem(), nonNullTestModel.get());
                testContext.assertEquals(testDate, updateResultAsyncResult.result().getItem().getSomeDateTwo());

                async.complete();
            });
        });
    }

    @Test
    public void delete(TestContext testContext) {
        Async async = testContext.async();
        final List<Future> futureList = new CopyOnWriteArrayList<>();

        createXItems(20, res -> {
            testContext.assertTrue(res.succeeded());

            res.result().stream().parallel().forEach(cr -> {
                final Future<Void> future = Future.future();
                final TestModel testModel = cr.getItem();
                final JsonObject id = new JsonObject()
                        .put("hash", testModel.getHash())
                        .put("range", testModel.getRange());

                repo.delete(id, deleteRes -> {
                    testContext.assertTrue(deleteRes.succeeded());

                    repo.read(id, readRes -> {
                        testContext.assertFalse(readRes.succeeded());

                        future.tryComplete();
                    });
                });

                futureList.add(future);
            });
        });

        CompositeFuture.all(futureList).setHandler(res -> {
            if (res.failed()) {
                testContext.fail(res.cause());
            } else {
                async.complete();
            }
        });
    }

    @Test
    public void read(TestContext testContext) {
        Async async = testContext.async();
        final List<Future> futureList = new CopyOnWriteArrayList<>();

        createXItems(20, res -> {
            testContext.assertTrue(res.succeeded());

            res.result().stream().parallel().forEach(cr -> {
                final Future<Void> future = Future.future();
                final TestModel testModel = cr.getItem();
                final JsonObject id = new JsonObject()
                        .put("hash", testModel.getHash())
                        .put("range", testModel.getRange());

                repo.read(id, firstRead -> {
                    testContext.assertTrue(firstRead.succeeded());

                    if (firstRead.succeeded()) {
                        repo.read(id, secondRead -> {
                            testContext.assertTrue(secondRead.succeeded());
                            testContext.assertTrue(secondRead.result().isCacheHit());

                            future.tryComplete();
                        });
                    } else {
                        future.tryComplete();
                    }
                });

                futureList.add(future);
            });
        });

        CompositeFuture.all(futureList).setHandler(res -> {
            if (res.failed()) {
                testContext.fail(res.cause());
            } else {
                async.complete();
            }
        });
    }

    @Test
    public void batchRead(TestContext testContext) {
        Async async = testContext.async();

        createXItems(20, res -> {
            testContext.assertTrue(res.succeeded());

            final List<TestModel> items = res.result().stream()
                    .map(CreateResult::getItem)
                    .collect(toList());

            final List<JsonObject> id = items.stream()
                    .map(testModel -> new JsonObject()
                            .put("hash", testModel.getHash())
                            .put("range", testModel.getRange()))
                    .collect(toList());

            repo.batchRead(id, batchRead -> {
                testContext.assertTrue(batchRead.succeeded());
                testContext.assertTrue(batchRead.result().size() == res.result().size());

                batchRead.result().stream()
                        .map(ItemResult::getItem)
                        .forEach(item -> testContext.assertTrue(items.contains(item)));

                async.complete();
            });
        });
    }

    @Test
    public void readWithConsistencyAndProjections(TestContext testContext) {
        Async async = testContext.async();
        final List<Future> futureList = new CopyOnWriteArrayList<>();

        createXItems(20, res -> {
            testContext.assertTrue(res.succeeded());

            res.result().stream().parallel().forEach(cr -> {
                final Future<Void> future = Future.future();
                final TestModel testModel = cr.getItem();
                final JsonObject id = new JsonObject()
                        .put("hash", testModel.getHash())
                        .put("range", testModel.getRange());

                repo.read(id, false, new String[]{"someLong"}, firstRead -> {
                    testContext.assertTrue(firstRead.succeeded());

                    if (firstRead.succeeded()) {
                        repo.read(id, false, new String[]{"someLong"}, secondRead -> {
                            testContext.assertTrue(secondRead.succeeded());
                            testContext.assertTrue(secondRead.result().isCacheHit());

                            future.tryComplete();
                        });
                    } else {
                        future.tryComplete();
                    }
                });

                futureList.add(future);
            });
        });

        CompositeFuture.all(futureList).setHandler(res -> {
            if (res.failed()) {
                testContext.fail(res.cause());
            } else {
                async.complete();
            }
        });
    }

    @Test
    public void readAll(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, res -> {
            testContext.assertTrue(res.succeeded());

            repo.readAll(allItemsRes -> {
                testContext.assertTrue(allItemsRes.succeeded());
                testContext.assertTrue(allItemsRes.result().size() == 100,
                        "Actual count: " + allItemsRes.result().size());

                async.complete();
            });
        });
    }

    @Test
    public void readAllWithIdentifiersAndFilterParameters(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, res -> {
            testContext.assertTrue(res.succeeded());
            final JsonObject idObject = new JsonObject()
                    .put("hash", "testString");
            final FilterParameter fp = FilterParameter.builder("someStringThree")
                    .withEq("1")
                    .build();
            final Map<String, List<FilterParameter>> fpList = new ConcurrentHashMap<>();
            fpList.put("someLong", Collections.singletonList(fp));

            repo.readAll(idObject, fpList, allItemsRes -> {
                testContext.assertTrue(allItemsRes.succeeded());
                testContext.assertTrue(allItemsRes.result().size() == 0, "Actual: " + allItemsRes.result().size());

                async.complete();
            });
        });
    }

    @Test
    public void readAllWithIdentifiersAndPageTokenAndQueryPackAndProjections(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, res -> {
            testContext.assertTrue(res.succeeded());
            final JsonObject idObject = new JsonObject()
                    .put("hash", "testString");

            pageAllResults(idObject, null, null, testContext, async);
        });
    }

    @Test
    public void readAllWithPageTokenAndQueryPackAndProjections(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, res -> {
            testContext.assertTrue(res.succeeded());
            pageAllResults(null, null, null, testContext, async);
        });
    }

    @Test
    public void readAllWithIdentifiersAndPageTokenAndQueryPackAndProjectionsAndGSI(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, res -> {
            testContext.assertTrue(res.succeeded());
            final JsonObject idObject = new JsonObject()
                    .put("hash", "testStringThree");

            pageAllResults(idObject, null, "TEST_GSI", testContext, async);
        });
    }

    private void pageAllResults(JsonObject idObject, String pageToken, String GSI,
                                TestContext testContext, Async async) {
        @SuppressWarnings("ConstantConditions")
        final QueryPack queryPack = QueryPack.builder(TestModel.class)
                .withPageToken(pageToken == null ? "NoToken" : pageToken)
                .build();
        final Handler<AsyncResult<ItemListResult<TestModel>>> handler = allItemsRes -> {
            if (allItemsRes.succeeded()) {
                final String finalPageToken = allItemsRes.result().getItemList().getPageToken();

                if (finalPageToken.equalsIgnoreCase("END_OF_LIST")) {
                    async.complete();
                } else {
                    pageAllResults(idObject, finalPageToken, GSI, testContext, async);
                }
            } else {
                testContext.fail("All Items Result is false!");

                async.complete();
            }
        };

        if (idObject == null) {
            if (GSI == null) {
                repo.readAll(pageToken, queryPack, new String[]{}, handler);
            } else {
                testContext.fail("Must use an idobject with GSI");

                async.complete();
            }
        } else {
            if (GSI != null) {
                repo.readAll(idObject, pageToken, queryPack, new String[]{}, GSI, handler);
            } else {
                repo.readAll(idObject, pageToken, queryPack, new String[]{}, handler);
            }
        }
    }

    @Test
    public void aggregation(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, allRes -> {
            final JsonObject idObject = new JsonObject()
                    .put("hash", "testString");
            final QueryPack queryPack = QueryPack.builder(TestModel.class)
                    .withAggregateFunction(AggregateFunction.builder()
                            .withAggregateFunction(AggregateFunctions.COUNT)
                            .withField("someStringOne")
                            .build())
                    .build();

            repo.aggregation(idObject, queryPack, new String[]{}, res -> {
                final Integer count = new JsonObject(res.result()).getInteger("count");

                testContext.assertEquals(100, count, "Count is: " + count);

                async.complete();
            });
        });
    }

    @Test
    public void aggregationGroupedRanged(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, allRes -> {
            final JsonObject idObject = new JsonObject()
                    .put("hash", "testString");
            final QueryPack queryPack = QueryPack.builder(TestModel.class)
                    .withAggregateFunction(AggregateFunction.builder()
                            .withAggregateFunction(MAX)
                            .withField("someLong")
                            .withGroupBy(Collections.singletonList(GroupingConfiguration.builder()
                                    .withGroupBy("someLong")
                                    .withGroupByUnit("INTEGER")
                                    .withGroupByRange(10000)
                                    .build()))
                            .build())
                    .build();

            AtomicInteger etagOne = new AtomicInteger();

            repo.aggregation(idObject, queryPack, new String[]{}, res -> {
                etagOne.set(res.result().hashCode());

                repo.aggregation(idObject, queryPack, new String[]{}, secondRes -> {
                    testContext.assertEquals(etagOne.get(), secondRes.result().hashCode());
                    async.complete();
                });
            });
        });
    }

    @Test
    public void aggregationWithGSI(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, allRes -> {
            final JsonObject idObject = new JsonObject()
                    .put("hash", "testStringThree");
            final QueryPack queryPack = QueryPack.builder(TestModel.class)
                    .withCustomQuery("TEST_GSI")
                    .withAggregateFunction(AggregateFunction.builder()
                            .withAggregateFunction(AggregateFunctions.COUNT)
                            .withField("someStringThree")
                            .build())
                    .build();

            repo.aggregation(idObject, queryPack, new String[]{}, "TEST_GSI", res -> {
                final JsonObject results = new JsonObject(res.result());
                final Integer count = results.getInteger("count");

                testContext.assertEquals(100, count, "Count is: " + count);

                async.complete();
            });
        });
    }

    @Test
    public void buildParameters(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithoutPagination(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, allItemsRes -> repo.readAllWithoutPagination("testString", allItems -> {
            testContext.assertTrue(allItems.succeeded());
            testContext.assertEquals(100, allItems.result().size(), "Size incorrect: " + allItems.result().size());

            async.complete();
        }));
    }

    @Test
    public void readAllWithoutPaginationWithIdentifierAndQueryPack(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, allItemsRes -> {
            final QueryPack queryPack = QueryPack.builder(TestModel.class)
                    .withAggregateFunction(AggregateFunction.builder()
                            .withAggregateFunction(AggregateFunctions.COUNT)
                            .withField("someStringOne")
                            .build())
                    .build();

            repo.readAllWithoutPagination("testString", queryPack, allItems -> {
                testContext.assertTrue(allItems.succeeded());
                testContext.assertEquals(100, allItems.result().size(), "Size incorrect: " + allItems.result().size());

                async.complete();
            });
        });
    }

    @Test
    public void readAllWithoutPaginationWithIdentifierAndQueryPackAndProjections(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, allItemsRes -> {
            final QueryPack queryPack = QueryPack.builder(TestModel.class)
                    .withAggregateFunction(AggregateFunction.builder()
                            .withAggregateFunction(AggregateFunctions.COUNT)
                            .withField("someStringOne")
                            .build())
                    .build();

            repo.readAllWithoutPagination("testString", queryPack, new String[]{}, allItems -> {
                testContext.assertTrue(allItems.succeeded());
                testContext.assertEquals(100, allItems.result().size(), "Size incorrect: " + allItems.result().size());

                async.complete();
            });
        });
    }

    @Test
    public void readAllWithoutPaginationWithIdentifierAndQueryPackAndProjectionsAndGSI(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, allItemsRes -> {
            final QueryPack queryPack = QueryPack.builder(TestModel.class)
                    .withCustomQuery("TEST_GSI")
                    .withAggregateFunction(AggregateFunction.builder()
                            .withAggregateFunction(AggregateFunctions.COUNT)
                            .withField("someStringOne")
                            .build())
                    .build();

            repo.readAllWithoutPagination("testStringThree", queryPack, new String[]{}, "TEST_GSI", allItems -> {
                testContext.assertTrue(allItems.succeeded());
                testContext.assertEquals(100, allItems.result().size(), "Size incorrect: " + allItems.result().size());

                async.complete();
            });
        });
    }

    @Test
    public void readAllWithoutPaginationWithQueryPackAndProjections(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, allItemsRes -> {
            final QueryPack queryPack = QueryPack.builder(TestModel.class)
                    .withAggregateFunction(AggregateFunction.builder()
                            .withAggregateFunction(AggregateFunctions.COUNT)
                            .withField("someStringOne")
                            .build())
                    .build();

            repo.readAllWithoutPagination(queryPack, new String[]{}, allItems -> {
                testContext.assertTrue(allItems.succeeded());
                testContext.assertEquals(100, allItems.result().size(), "Size incorrect: " + allItems.result().size());

                async.complete();
            });
        });
    }

    @Test
    public void readAllWithoutPaginationWithQueryPackAndProjectionsAndGSI(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, allItemsRes -> {
            final QueryPack queryPack = QueryPack.builder(TestModel.class)
                    .withCustomQuery("TEST_GSI")
                    .withAggregateFunction(AggregateFunction.builder()
                            .withAggregateFunction(AggregateFunctions.COUNT)
                            .withField("someStringThree")
                            .build())
                    .build();

            repo.readAllWithoutPagination(queryPack, new String[]{}, "TEST_GSI", allItems -> {
                testContext.assertTrue(allItems.succeeded());
                testContext.assertEquals(100, allItems.result().size(), "Size incorrect: " + allItems.result().size());

                async.complete();
            });
        });
    }

    @Test
    public void readAllPaginated(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, allItemsRes -> repo.readAllPaginated(allItems -> {
            testContext.assertTrue(allItems.succeeded());
            testContext.assertEquals(100, allItems.result().size(), "Size incorrect: " + allItems.result().size());

            async.complete();
        }));
    }

    @Test
    public void remoteCreate(TestContext testContext) {
        Async async = testContext.async();

        service.remoteCreate(nonNullTestModel.get(), createRes -> {
            testContext.assertTrue(createRes.succeeded());
            testContext.assertEquals(createRes.result().getHash(), nonNullTestModel.get().getHash());
            testContext.assertEquals(createRes.result().getRange(), nonNullTestModel.get().getRange());

            async.complete();
        });
    }

    @Test
    public void remoteRead(TestContext testContext) {
        Async async = testContext.async();
        final List<Future> futureList = new CopyOnWriteArrayList<>();

        createXItems(20, res -> {
            testContext.assertTrue(res.succeeded());

            res.result().stream().parallel().forEach(cr -> {
                final Future<Void> future = Future.future();
                final TestModel testModel = cr.getItem();
                final JsonObject id = new JsonObject()
                        .put("hash", testModel.getHash())
                        .put("range", testModel.getRange());

                service.remoteRead(id, firstRead -> {
                    testContext.assertTrue(firstRead.succeeded());

                    future.tryComplete();
                });

                futureList.add(future);
            });
        });

        CompositeFuture.all(futureList).setHandler(res -> {
            if (res.failed()) {
                testContext.fail(res.cause());
            } else {
                async.complete();
            }
        });
    }

    @Test
    public void remoteIndex(TestContext testContext) {
        Async async = testContext.async();

        createXItems(100, res -> {
            testContext.assertTrue(res.succeeded());
            final JsonObject idObject = new JsonObject()
                    .put("hash", "testString");

            service.remoteIndex(idObject, allItemsRes -> {
                testContext.assertTrue(allItemsRes.succeeded());
                testContext.assertTrue(allItemsRes.result().size() == 100,
                        "Actual count: " + allItemsRes.result().size());

                async.complete();
            });
        });
    }

    @Test
    public void remoteUpdate(TestContext testContext) {
        Async async = testContext.async();

        service.remoteCreate(nonNullTestModel.get(), createRes -> {
            testContext.assertTrue(createRes.succeeded());
            testContext.assertEquals(createRes.result().getHash(), nonNullTestModel.get().getHash());
            testContext.assertEquals(createRes.result().getRange(), nonNullTestModel.get().getRange());

            Date testDate = new Date();
            final TestModel result = createRes.result();

            service.remoteUpdate(result, updateRes -> {
                testContext.assertTrue(updateRes.succeeded());
                testContext.assertEquals(updateRes.result().getSomeDateTwo(), testDate);

                async.complete();
            });
        });
    }

    @Test
    public void remoteDelete(TestContext testContext) {
        Async async = testContext.async();

        service.remoteCreate(nonNullTestModel.get(), createRes -> {
            testContext.assertTrue(createRes.succeeded());
            testContext.assertEquals(createRes.result().getHash(), nonNullTestModel.get().getHash());
            testContext.assertEquals(createRes.result().getRange(), nonNullTestModel.get().getRange());
            final JsonObject id = new JsonObject()
                    .put("hash", createRes.result().getHash())
                    .put("range", createRes.result().getRange());

            service.remoteDelete(id, deleteRes -> {
                testContext.assertTrue(deleteRes.succeeded());

                service.remoteRead(id, res -> {
                    testContext.assertFalse(res.succeeded());

                    async.complete();
                });
            });
        });
    }

    @Test
    public void getModelName() {
        assertEquals("TestModel", repo.getModelName());
    }

    @Test
    public void createS3Link() {
        final S3Link test = DynamoDBRepository.createS3Link(repo.getDynamoDbMapper(), "someName", "/someBogusPath");

        assertNotNull(test);
        assertEquals("Path is not equal!", "/someBogusPath", test.getKey());
    }

    @Test
    public void createSignedUrl() {
        final S3Link test = DynamoDBRepository.createS3Link(repo.getDynamoDbMapper(), "someName", "/someBogusPath");
        String signedUrl = DynamoDBRepository.createSignedUrl(repo.getDynamoDbMapper(), test);

        assertNotNull("Url is null!", signedUrl);
        assertTrue("Url is not secure: " + signedUrl, signedUrl.startsWith("https://s3"));
        assertTrue("Url is not secure: " + signedUrl, signedUrl.contains("X-Amz-Algorithm"));
    }

    @Test
    public void createSignedUrlWithDays() {
        final S3Link test = DynamoDBRepository.createS3Link(repo.getDynamoDbMapper(), "someName", "/someBogusPath");
        String signedUrl = DynamoDBRepository.createSignedUrl(repo.getDynamoDbMapper(), 7, test);

        assertNotNull("Url is null!", signedUrl);
        assertTrue("Url is not secure: " + signedUrl, signedUrl.startsWith("https://s3"));
        assertTrue("Url is not secure: " + signedUrl, signedUrl.contains("X-Amz-Algorithm"));
    }

    @Test
    public void buildEventbusProjections() {
        final JsonArray array = new JsonArray()
                .add("someStringOne")
                .add("someLong");

        repo.buildEventbusProjections(array);
    }

    @Test
    public void hasRangeKey() {
        assertTrue(repo.hasRangeKey());
    }

    @Test
    public void getDynamoDbMapper() {
        assertNotNull(repo.getDynamoDbMapper());
    }

    @Test
    public void getRedisClient(TestContext testContext) {
        Async async = testContext.async();

        if (config.getString("redis_host") != null) {
            testContext.assertNotNull(repo.getRedisClient());
            repo.getRedisClient().info(info -> async.complete());
        } else {
            testContext.assertNull(repo.getRedisClient());

            async.complete();
        }
    }
}