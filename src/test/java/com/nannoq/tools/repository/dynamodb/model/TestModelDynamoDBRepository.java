package com.nannoq.tools.repository.dynamodb.model;

import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import com.nannoq.tools.repository.dynamodb.service.TestModelInternalService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class TestModelDynamoDBRepository extends DynamoDBRepository<TestModel> implements TestModelInternalService {
    public TestModelDynamoDBRepository(JsonObject appConfig) {
        super(TestModel.class, appConfig);
    }

    public TestModelDynamoDBRepository(Vertx vertx, JsonObject config) {
        super(vertx, TestModel.class, config);
    }

    @Override
    public TestModelDynamoDBRepository remoteIndex(JsonObject identifier,
                                                   Handler<AsyncResult<List<TestModel>>> asyncResultHandler) {
        super.remoteIndex(identifier, asyncResultHandler);

        return this;
    }
}
