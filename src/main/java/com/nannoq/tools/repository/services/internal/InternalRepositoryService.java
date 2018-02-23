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

package com.nannoq.tools.repository.services.internal;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.function.Function;

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
    InternalRepositoryService<T> remoteUpdate(T record, Function<T, T> updateLogic, Handler<AsyncResult<T>> resultHandler);

    @Fluent
    InternalRepositoryService<T> remoteDelete(JsonObject identifiers, Handler<AsyncResult<T>> resultHandler);
}