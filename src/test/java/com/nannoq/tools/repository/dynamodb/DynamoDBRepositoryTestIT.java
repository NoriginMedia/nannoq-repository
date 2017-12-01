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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.S3Link;
import com.amazonaws.services.dynamodbv2.model.*;
import com.hazelcast.config.Config;
import com.nannoq.tools.repository.dynamodb.model.TestModel;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import redis.embedded.RedisServer;

import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import static com.nannoq.tools.repository.dynamodb.DynamoDBRepository.PAGINATION_INDEX;
import static com.nannoq.tools.repository.repository.Repository.logger;
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
            .put("dynamo_endpoint", System.getProperty("dynamo.endpoint"))
            .put("redis_host", System.getProperty("redis.endpoint"))
            .put("redis_port", Integer.parseInt(System.getProperty("redis.port")))
            .put("dynamo_db_iam_id", "someTestId")
            .put("dynamo_db_iam_key", "someTestKey")
            .put("content_bucket", "someName");

    private final Date testDate = new Date();
    private final TestModel nonNullTestModel = new TestModel()
            .setSomeStringOne("testString")
            .setSomeStringTwo("testStringRange")
            .setSomeLong(1L)
            .setSomeDate(testDate);

    private static Vertx vertx;
    private RedisServer redisServer;
    private DynamoDBRepository<TestModel> repo;
    private final String tableName = TestModel.class.getAnnotation(DynamoDBTable.class).tableName();
    private final Map<String, Class> testMap = Collections.singletonMap(tableName, TestModel.class);

    @Rule public TestName name = new TestName();

    @BeforeClass
    public static void setUpClass(TestContext testContext) {
        Async async = testContext.async();

        Config hzConfig = new Config() ;
        hzConfig.setProperty( "hazelcast.logging.type", "log4j2" );
        HazelcastClusterManager mgr = new HazelcastClusterManager();
        mgr.setConfig(hzConfig);
        VertxOptions options = new VertxOptions().setClusterManager(mgr);

        Vertx.clusteredVertx(options, clustered -> {
            if (clustered.failed()) {
                System.out.println("Vertx not able to cluster!");

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
                repo = new DynamoDBRepository<>(vertx, TestModel.class, config);
            }

            long redisChecker = System.currentTimeMillis();

            while (!redisServer.isActive()) {
                if (System.currentTimeMillis() > redisChecker + 10000) {
                    logger.error("No connection with Redis, terminating!");

                    System.exit(-1);
                }
            }

            async.complete();
        });
    }

    @After
    public void tearDown() {
        final AmazonDynamoDBAsyncClient amazonDynamoDBAsyncClient = new AmazonDynamoDBAsyncClient();
        amazonDynamoDBAsyncClient.withEndpoint(config.getString("dynamo_endpoint"));
        amazonDynamoDBAsyncClient.deleteTable(tableName);

        repo = null;
        redisServer.stop();
        redisServer = null;


        logger.info("Closing " + name.getMethodName());
    }

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
        assertEquals("BucketName does not match Config!", config.getString("content_bucket"), DynamoDBRepository.getBucketName());
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
        assertNotNull("FieldAsObject is null!", repo.getFieldAsObject("someStringOne", nonNullTestModel));
    }

    @Test
    public void getFieldAsString() {
        assertNotNull("FieldAsString is null!", repo.getField("someStringOne"));
        assertEquals(repo.getFieldAsString("someStringOne", nonNullTestModel).getClass(), String.class);
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
        AttributeValue attributeValue = repo.getIndexValue("someDate", nonNullTestModel);
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

        repo.create(nonNullTestModel).setHandler(res -> {
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

        repo.create(nonNullTestModel).setHandler(res -> {
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

        repo.create(nonNullTestModel).setHandler(res -> {
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
    public void read(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readWithConsistencyAndProjections(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAll(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithIdentifiersAndFilterParameters(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithIdentifiersAndPageTokenAndQueryPackAndProjections(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithPageTokenAndQueryPackAndProjections(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithIdentifiersAndPageTokenAndQueryPackAndProjectionsAndGSI(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void aggregation(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void aggregationWithGSI(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void buildParameters(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithoutPagination(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithoutPaginationWithIdentifierAndQueryPack(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithoutPaginationWithIdentifierAndQueryPackAndProjections(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithoutPaginationWithIdentifierAndQueryPackAndProjectionsAndGSI(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithoutPaginationWithQueryPackAndProjections(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllWithoutPaginationWithQueryPackAndProjectionsAndGSI(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void readAllPaginated(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void doWriteCreate(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void doWriteUpdate(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void doDelete(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void buildCollectionEtagKey() {
        assertEquals("data_api_testModels_s_etag", repo.buildCollectionEtagKey());
    }

    @Test
    public void getEtags(TestContext testContext) {
        Async async = testContext.async();

        repo.getEtags(etags -> {
            if (etags.failed()) {
                testContext.fail(etags.cause());
                async.complete();
            } else {
                async.complete();
            }
        });
    }

    @Test
    public void remoteCreate(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void remoteRead(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void remoteIndex(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void remoteUpdate(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void remoteDelete(TestContext testContext) {
        Async async = testContext.async();

        async.complete();
    }

    @Test
    public void getModelName() {
        assertEquals("TestModel", repo.getModelName());
    }

    @Test
    public void createS3Link() {
        final S3Link test = DynamoDBRepository.createS3Link(repo.getDynamoDbMapper(), "/someBogusPath");

        assertNotNull(test);
        assertEquals("Path is not equal!", "/someBogusPath", test.getKey());
    }

    @Test
    public void createSignedUrl() {
        final S3Link test = DynamoDBRepository.createS3Link(repo.getDynamoDbMapper(), "/someBogusPath");
        String signedUrl = DynamoDBRepository.createSignedUrl(repo.getDynamoDbMapper(), test);

        assertNotNull("Url is null!", signedUrl);
        assertTrue("Url is not secure: " + signedUrl, signedUrl.startsWith("https://s3"));
        assertTrue("Url is not secure: " + signedUrl, signedUrl.contains("X-Amz-Algorithm"));
    }

    @Test
    public void createSignedUrlWithDays() {
        final S3Link test = DynamoDBRepository.createS3Link(repo.getDynamoDbMapper(), "/someBogusPath");
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

        testContext.assertNotNull(repo.getRedisClient());

        repo.getRedisClient().info(info -> async.complete());
    }
}