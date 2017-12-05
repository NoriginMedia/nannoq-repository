package com.nannoq.tools.repository.dynamodb.service;

import com.nannoq.tools.repository.dynamodb.model.TestModel;
import com.nannoq.tools.repository.services.internal.InternalRepositoryService;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@ProxyGen
@VertxGen
public interface TestModelInternalService extends InternalRepositoryService<TestModel> {
    @Override
    @Fluent
    TestModelInternalService remoteIndex(JsonObject identifier,
                                         Handler<AsyncResult<List<TestModel>>> asyncResultHandler);
}
