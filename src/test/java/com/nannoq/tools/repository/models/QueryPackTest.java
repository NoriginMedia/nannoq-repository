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

import com.nannoq.tools.repository.dynamodb.model.TestModel;
import com.nannoq.tools.repository.utils.*;
import org.junit.Test;

import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.nannoq.tools.repository.dynamodb.DynamoDBRepository.PAGINATION_INDEX;
import static com.nannoq.tools.repository.utils.AggregateFunctions.COUNT;
import static com.nannoq.tools.repository.utils.AggregateFunctions.MAX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class QueryPackTest {
    @Test
    public void getBaseEtagKey() throws Exception {
        QueryPack queryPack = QueryPack.builder(TestModel.class).build();

        assertNotNull(queryPack.getBaseEtagKey());
    }

    @Test
    public void testBaseKey() throws Exception {
        QueryPack queryPack = makePack("NoTag", null);
        QueryPack queryPackTwo = makePack("62ecb02196f36ddbb81c72b1513bb81", null);

        assertEquals(queryPack.getBaseEtagKey(), queryPackTwo.getBaseEtagKey());
    }

    private QueryPack makePack(String etag, String pageToken) {
        Queue<OrderByParameter> queue = new ConcurrentLinkedDeque<>();
        queue.add(OrderByParameter.builder()
                .withField("someLong")
                .build());

        AggregateFunction aggregateFunction = AggregateFunction.builder()
                .withAggregateFunction(MAX)
                .withField("someLong")
                .withGroupBy(Collections.singletonList(GroupingConfiguration.builder()
                        .withGroupBy("someLong")
                        .withGroupByUnit("INTEGER")
                        .withGroupByRange(10000)
                        .build()))
                .build();

        return QueryPack.builder(TestModel.class)
                .withCustomRoute("/parent/testString/testModels")
                .withCustomQuery(null)
                .withPageToken(pageToken)
                .withRequestEtag(etag)
                .withOrderByQueue(queue)
                .withFilterParameters(Collections.singletonMap("someBoolean",
                        Collections.singletonList(FilterParameter.builder("someBoolean")
                                .withEq("true")
                                .build())))
                .withAggregateFunction(aggregateFunction)
                .withProjections(new String[]{"someStringOne"})
                .withIndexName(PAGINATION_INDEX)
                .withLimit(0)
                .build();
    }
}