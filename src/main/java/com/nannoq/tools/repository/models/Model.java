package com.nannoq.tools.repository.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import java.util.Date;
import java.util.List;

/**
 * File: Model
 * Project: gcm-backend
 * Package: com.noriginmedia.norigintube.model.utils
 * <p>
 * This interface
 *
 * @author anders
 * @version 4/6/16
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface Model {
    String getId();
    Model setModifiables(Model newObject);

    @Fluent
    Model sanitize();
    List<ValidationError> validateCreate();
    List<ValidationError> validateUpdate();


    Date getCreatedAt();
    Model setCreatedAt(Date date);
    Date getUpdatedAt();
    Model setUpdatedAt(Date date);

    @Fluent
    Model setInitialValues(Model record);

    JsonObject toJsonFormat(@Nonnull String[] projections);

    default JsonObject toJsonFormat() { return toJsonFormat(new String[]{}); }

    default String toJsonString() {
        return Json.encode(toJsonFormat());
    }

    default String toJsonString(@Nonnull String[] projections) {
        return Json.encode(toJsonFormat(projections));
    }

    default JsonObject validateNotNullAndAdd(JsonObject jsonObject, @Nonnull List<String> projectionList,
                                             String key, Object value) {
        if (value != null) {
            if (projectionList.isEmpty() || projectionList.contains(key)) {
                jsonObject.put(key, value);
            }
        }

        return jsonObject;
    }

    static JsonObject buildValidationErrorObject(List<ValidationError> errors) {
        JsonObject errorObject = new JsonObject();
        errorObject.put("error_type", "VALIDATION");
        JsonArray errorObjects = new JsonArray();
        errors.stream().map(ValidationError::toJson).forEach(errorObjects::add);

        errorObject.put("errors", errorObjects);

        return errorObject;
    }
}
