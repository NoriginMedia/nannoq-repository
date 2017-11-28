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