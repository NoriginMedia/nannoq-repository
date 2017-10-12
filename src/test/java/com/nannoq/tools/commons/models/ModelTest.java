package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.models.utils.FilterParameterTestClass;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ModelTest {
    @Test
    public void validateNotNullAndAdd() throws Exception {
        JsonObject json = new JsonObject();
        new FilterParameterTestClass().validateNotNullAndAdd(json, Collections.emptyList(), "lol", null);
        assertTrue(json.isEmpty());
        json = new JsonObject();
        new FilterParameterTestClass().validateNotNullAndAdd(json, Collections.emptyList(), "lol", "lol");
        assertFalse(json.isEmpty());
        json = new JsonObject();
        new FilterParameterTestClass().validateNotNullAndAdd(json, Collections.singletonList("lolNo"), "lol", "lol");
        assertTrue(json.isEmpty());
        json = new JsonObject();
        new FilterParameterTestClass().validateNotNullAndAdd(json, Collections.singletonList("lol"), "lol", "lol");
        assertFalse(json.isEmpty());
    }
}