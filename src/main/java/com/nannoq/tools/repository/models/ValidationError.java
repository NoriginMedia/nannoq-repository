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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Calendar;
import java.util.Date;

/**
 * This class defines helpers for Model operations.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@DataObject(generateConverter = true)
public class ValidationError {
    @JsonIgnore
    private static final long DAY = 86400000L;

    private String description;
    private String fieldName;

    public ValidationError() {
    }

    public ValidationError(String description, String fieldName) {
        this.description = description;
        this.fieldName = fieldName;
    }

    public ValidationError(JsonObject jsonObject) {
        description = jsonObject.getString("description");
        fieldName = jsonObject.getString("fieldName");
    }

    public JsonObject toJson() {
        return JsonObject.mapFrom(this);
    }

    public String getDescription() {
        return description;
    }

    public String getFieldName() {
        return fieldName;
    }

    public static ValidationError validateNotNull(Object o, String fieldName) {
        return o == null ? new ValidationError("Cannot be null!", fieldName) : null;
    }

    public static ValidationError validateDate(Date date, String fieldName) {
        if (date == null) return new ValidationError("Date cannot be null!", fieldName);

        Calendar yesterday = Calendar.getInstance();
        yesterday.setTimeInMillis(yesterday.getTime().getTime() - DAY);
        
        if (date != null && date.before(yesterday.getTime())) {
            return new ValidationError("Cannot be older than 24H!", fieldName);
        }

        return null;
    }

    public static ValidationError validateTextLength(String field, String fieldName, int count) {
        if (field == null) return new ValidationError("Field cannot be null!", fieldName);

        if (field != null && field.length() > count) {
            return new ValidationError("Cannot be over " + count + " characters!", fieldName);
        }

        return null;
    }
}
