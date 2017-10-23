package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.utils.QueryPack;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueryPackTest {
    @Test
    public void getBaseEtagKey() throws Exception {
        String onlyRoute = ModelUtils.returnNewEtag("route".hashCode());
        String withQuery = ModelUtils.returnNewEtag("route".hashCode() + "query".hashCode());

        QueryPack queryPackRoute = QueryPack.builder()
                .withRoute("route")
                .build();

        QueryPack queryPackBoth = QueryPack.builder()
                .withRoute("route")
                .withQuery("query")
                .build();

        assertEquals(onlyRoute, queryPackRoute.getBaseEtagKey());
        assertEquals(withQuery, queryPackBoth.getBaseEtagKey());
    }
}