package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.utils.ItemList;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ItemListTest {
    @Test
    public void toJson() throws Exception {
        final JsonObject result = new ItemList<>().toJson(new String[]{});

        assertNotNull(result.getInteger("count"));
        assertNotNull(result.getString("etag"));
        assertNotNull(result.getString("pageToken"));
        assertNotNull(result.getJsonArray("items"));
        assertTrue(result.getJsonArray("items").isEmpty());
    }
}