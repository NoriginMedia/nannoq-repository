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
 * This class defines the model interface which includes sanitation, validation and json representations.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface Model {
    Model setModifiables(Model newObject);

    @Fluent
    Model sanitize();
    List<ValidationError> validateCreate();
    List<ValidationError> validateUpdate();

    Model setIdentifiers(JsonObject identifiers);

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
