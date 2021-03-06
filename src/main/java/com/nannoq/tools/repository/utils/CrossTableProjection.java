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

package com.nannoq.tools.repository.utils;

import com.nannoq.tools.repository.models.ValidationError;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class defines projection configurations for cross table queries.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class CrossTableProjection {
    private List<String> TABLES = null;

    private List<String> models;
    private List<String> fields;

    public CrossTableProjection() {
        this(null, null);
    }

    public CrossTableProjection(List<String> models, List<String> TABLES) {
        this(models, TABLES, null);
    }

    public CrossTableProjection(List<String> models, List<String> TABLES, List<String> fields) {
        this.models = models;
        this.TABLES = TABLES;
        this.fields = fields;
    }

    public List<String> getModels() {
        return models;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setTABLES(List<String> TABLES) {
        this.TABLES = TABLES;
    }

    public List<ValidationError> validate(AggregateFunctions function) {
        List<ValidationError> errors = new ArrayList<>();

        if (models == null) {
            errors.add(new ValidationError(
                    "models_error", "Models cannot be null for!"));
        } else {
            models.forEach(model -> {
                if (!TABLES.contains(model)) {
                    errors.add(new ValidationError(
                            model + " is not a valid model!",
                            "models_" + model));
                }
            });
        }

        if (fields == null && function != AggregateFunctions.COUNT) {
            errors.add(new ValidationError(
                    "fields_error", "Fields cannot be null for: " + function.name()));
        } else if (fields != null && function != AggregateFunctions.COUNT) {
            fields.forEach(field -> {
                if (StringUtils.countMatches(field, ".") != 1) {
                    errors.add(new ValidationError(
                            field + " invalid! Must be in this format: <modelNamePluralized>.<fieldName>",
                            "fields_" + field));
                }

                final String[] fieldSplit = field.split("\\.");

                if (!fieldSplit[0].endsWith("s")) {
                    errors.add(new ValidationError(
                            field + " is not pluralized!", "fields_" + field));
                }
            });
        } else if (fields != null) {
            errors.add(new ValidationError(
                    "fields_error", "Fields must be null for: " + function.name()));
        }

        return errors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CrossTableProjection that = (CrossTableProjection) o;

        return Objects.equals(TABLES, that.TABLES) &&
                Objects.equals(models, that.models) &&
                Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(TABLES, models, fields);
    }
}
