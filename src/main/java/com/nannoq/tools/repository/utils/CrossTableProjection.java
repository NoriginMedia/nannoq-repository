package com.nannoq.tools.repository.utils;

import com.nannoq.tools.repository.models.ValidationError;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

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
}
