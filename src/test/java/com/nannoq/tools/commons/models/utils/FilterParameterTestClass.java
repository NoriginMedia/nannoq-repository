package com.nannoq.tools.repository.models.utils;

import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.models.ValidationError;
import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import java.util.Date;
import java.util.List;

public class FilterParameterTestClass implements Model {
    private Long viewCount;

    @Override
    public String getId() {
        return null;
    }

    @Override
    public Model setModifiables(Model newObject) {
        return null;
    }

    @Override
    public Model sanitize() {
        return null;
    }

    @Override
    public List<ValidationError> validateCreate() {
        return null;
    }

    @Override
    public List<ValidationError> validateUpdate() {
        return null;
    }

    @Override
    public Date getCreatedAt() {
        return null;
    }

    @Override
    public Model setCreatedAt(Date date) {
        return null;
    }

    @Override
    public Date getUpdatedAt() {
        return null;
    }

    @Override
    public Model setUpdatedAt(Date date) {
        return null;
    }

    @Override
    public Model setInitialValues(Model record) {
        return null;
    }

    @Override
    public JsonObject toJsonFormat(@Nonnull String[] projections) {
        return null;
    }
}
