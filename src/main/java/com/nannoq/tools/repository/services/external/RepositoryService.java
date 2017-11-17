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