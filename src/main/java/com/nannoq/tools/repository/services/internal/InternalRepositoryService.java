package com.nannoq.tools.repository.services.internal;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * This interface declares a contract for the internal facing repository services.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@VertxGen(concrete = false)
public interface InternalRepositoryService<T> {
    @Fluent
    InternalRepositoryService<T> remoteCreate(T record, Handler<AsyncResult<T>> resultHandler);

    @Fluent
    InternalRepositoryService<T> remoteRead(JsonObject identifiers, Handler<AsyncResult<T>> resultHandler);

    @GenIgnore
    @Fluent
    InternalRepositoryService<T> remoteIndex(JsonObject identifier, Handler<AsyncResult<List<T>>> resultHandler);

    @Fluent
    InternalRepositoryService<T> remoteUpdate(T record, Handler<AsyncResult<T>> resultHandler);

    @Fluent
    InternalRepositoryService<T> remoteDelete(JsonObject identifiers, Handler<AsyncResult<T>> resultHandler);
}