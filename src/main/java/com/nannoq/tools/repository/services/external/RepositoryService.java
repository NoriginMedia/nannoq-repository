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

package com.nannoq.tools.repository.services.external;

import com.nannoq.tools.repository.models.ValidationError;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.List;

/**
 * This interface declares a contract for the public facing repository services, with standardized event messages.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public interface RepositoryService<T> {
    @GenIgnore
    Logger SERVICE_LOGGER = LoggerFactory.getLogger(RepositoryService.class.getSimpleName());

    @GenIgnore
    default void sendCreateEvent(Vertx vertx, String address, String modelName, JsonObject result) {
        vertx.eventBus().publish(address, new JsonObject()
                .put("action", "CREATE")
                .put("model", modelName)
                .put("item", result));
    }

    @GenIgnore
    default void sendUpdateEvent(Vertx vertx, String address, String modelName, JsonObject result) {
        vertx.eventBus().publish(address, new JsonObject()
                .put("action", "UPDATE")
                .put("model", modelName)
                .put("item", result));
    }

    @GenIgnore
    default void sendDeleteEvent(Vertx vertx, String address, String modelName, JsonObject result) {
        vertx.eventBus().publish(address, new JsonObject()
                .put("action", "DELETE")
                .put("model", modelName)
                .put("item", result));
    }

    @GenIgnore
    default JsonObject getErrorsAsJson(List<ValidationError> errors) {
        JsonObject errorObject = new JsonObject();
        JsonArray errorsArray = new JsonArray();
        errors.forEach(e -> errorsArray.add(e.toJson()));
        errorObject.put("errors", errorsArray);

        return errorObject;
    }
}