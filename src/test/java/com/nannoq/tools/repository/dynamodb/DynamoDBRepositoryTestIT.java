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
import com.hazelcast.config.Config;
import com.nannoq.tools.repository.dynamodb.model.TestModel;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.*;
import org.junit.runner.RunWith;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * This class defines DynamoDBRepository class. It handles almost all cases of use with the DynamoDB of AWS.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@RunWith(VertxUnitRunner.class)
public class DynamoDBRepositoryTestIT {
    private final JsonObject config = new JsonObject()
            .put("dynamo_endpoint", System.getProperty("dynamo.endpoint"))
            .put("redis_host", System.getProperty("redis.endpoint"))
            .put("dynamo_db_iam_id", "someTestId")
            .put("dynamo_db_iam_key", "someTestKey")
            .put("content_bucket", "someName");

    private final TestModel nonNullTestModel = new TestModel()
            .setSomeStringOne("testString");

    private static Vertx vertx;
    private RedisServer redisServer;
    private DynamoDBRepository<TestModel> testModelDynamoDBRepository;
    private final String tableName = TestModel.class.getAnnotation(DynamoDBTable.class).tableName();
    private final Map<String, Class> testMap = Collections.singletonMap(tableName, TestModel.class);

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
            }

            async.complete();
        });
    }

    @Before
    public void setUp() throws Exception {
        redisServer = new RedisServer(Integer.parseInt(System.getProperty("redis.port")));
        redisServer.start();
        DynamoDBRepository.initializeDynamoDb(config, testMap);
        testModelDynamoDBRepository = new DynamoDBRepository<>(vertx, TestModel.class, config);
    }

    @After
    public void tearDown() throws Exception {
        final AmazonDynamoDBAsyncClient amazonDynamoDBAsyncClient = new AmazonDynamoDBAsyncClient();
        amazonDynamoDBAsyncClient.withEndpoint(config.getString("dynamo_endpoint"));
        amazonDynamoDBAsyncClient.listTablesAsync().get().getTableNames()
                .forEach(amazonDynamoDBAsyncClient::deleteTable);
        testModelDynamoDBRepository = null;
        redisServer.stop();
        redisServer = null;
    }

    @AfterClass
    public static void tearDownClass() {
        vertx.close();
    }

    @Test
    public void getBucketName() {
        assertEquals("BucketName does not match Config!", config.getString("content_bucket"), DynamoDBRepository.getBucketName());
    }

    @Test
    public void stripGet() {
        assertEquals("stripGet does not strip correctly!", "someStringOne", testModelDynamoDBRepository.stripGet("getSomeStringOne"));
    }

    @Test
    public void getField() {
        assertNotNull("Field is null!", testModelDynamoDBRepository.getField("someStringOne"));
    }

    @Test(expected = UnknownError.class)
    public void getFieldFail() {
        testModelDynamoDBRepository.getField("someBogusField");
    }

    @Test
    public void getFieldAsObject() {
        assertNotNull("FieldAsObject is null!", testModelDynamoDBRepository.getFieldAsObject("someStringOne", nonNullTestModel));
    }

    @Test
    public void getFieldAsString() {
        assertNotNull("FieldAsString is null!", testModelDynamoDBRepository.getField("someStringOne"));
        assertEquals(testModelDynamoDBRepository.getFieldAsString("someStringOne", nonNullTestModel).getClass(), String.class);
    }

    @Test
    public void checkAndGetField() {
        assertNotNull("CheckAndGetField is null!", testModelDynamoDBRepository.checkAndGetField("someLong"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkAndGetFieldNonIncrementable() {
        assertNotNull("CheckAndGetField is null!", testModelDynamoDBRepository.checkAndGetField("someStringOne"));
    }

    @Test
    public void hasField() {
        final Field[] declaredFields = TestModel.class.getDeclaredFields();

        assertTrue(testModelDynamoDBRepository.hasField(declaredFields, "someStringOne"));
        assertFalse(testModelDynamoDBRepository.hasField(declaredFields, "someBogusField"));
    }

    @Test
    public void getAlternativeIndexIdentifier() {
    }

    @Test
    public void getIndexValue() {
    }

    @Test
    public void createAttributeValue() {
    }

    @Test
    public void createAttributeValue1() {
    }

    @Test
    public void fetchNewestRecord() {
    }

    @Test
    public void buildExpectedAttributeValue() {
    }

    @Test
    public void incrementField() {
    }

    @Test
    public void decrementField() {
    }

    @Test
    public void read() {
    }

    @Test
    public void read1() {
    }

    @Test
    public void readAll() {
    }

    @Test
    public void readAll1() {
    }

    @Test
    public void readAll2() {
    }

    @Test
    public void readAll3() {
    }

    @Test
    public void readAll4() {
    }

    @Test
    public void aggregation() {
    }

    @Test
    public void aggregation1() {
    }

    @Test
    public void buildParameters() {
    }

    @Test
    public void readAllWithoutPagination() {
    }

    @Test
    public void readAllWithoutPagination1() {
    }

    @Test
    public void readAllWithoutPagination2() {
    }

    @Test
    public void readAllWithoutPagination3() {
    }

    @Test
    public void readAllWithoutPagination4() {
    }

    @Test
    public void readAllWithoutPagination5() {
    }

    @Test
    public void readAllPaginated() {
    }

    @Test
    public void doWrite() {
    }

    @Test
    public void doDelete() {
    }

    @Test
    public void buildCollectionEtagKey() {
    }

    @Test
    public void getEtags() {
    }

    @Test
    public void remoteCreate() {
    }

    @Test
    public void remoteRead() {
    }

    @Test
    public void remoteIndex() {
    }

    @Test
    public void remoteUpdate() {
    }

    @Test
    public void remoteDelete() {
    }

    @Test
    public void getModelName() {
    }

    @Test
    public void initializeDynamoDb() {
    }

    @Test
    public void createS3Link() {
    }

    @Test
    public void createSignedUrl() {
    }

    @Test
    public void createSignedUrl1() {
    }

    @Test
    public void buildEventbusProjections() {
    }

    @Test
    public void hasRangeKey() {
    }

    @Test
    public void getDynamoDbMapper() {
    }

    @Test
    public void getRedisClient() {
    }
}