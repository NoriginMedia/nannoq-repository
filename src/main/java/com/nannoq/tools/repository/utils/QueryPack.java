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

import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.models.ModelUtils;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;

import java.util.*;

/**
 * This class defines the querypack. A querypack includes the orderByQueue, the map of filterparameters to be performed,
 * and any aggregate function.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class QueryPack {
    private String query;
    private String baseEtagKey;
    private String route;
    private String pageToken;
    private String requestEtag;
    private Queue<OrderByParameter> orderByQueue;
    private Map<String, List<FilterParameter>> params;
    private AggregateFunction aggregateFunction;
    private String[] projections;
    private String indexName;
    private Integer limit;

    private QueryPack() {}

    public static QueryPackBuilder builder() {
        return builder(null);
    }

    public static QueryPackBuilder builder(Class model) {
        return new QueryPackBuilder(model);
    }

    public static class QueryPackBuilder {
        private static Logger logger = LoggerFactory.getLogger(QueryPackBuilder.class.getSimpleName());

        private String query;
        private String route;
        private String pageToken;
        private String requestEtag;
        private Queue<OrderByParameter> orderByQueue;
        private Map<String, List<FilterParameter>> params;
        private AggregateFunction aggregateFunction;
        private String[] projections;
        private String indexName;
        private Integer limit;

        private QueryPackBuilder(Class model) {
            if (model != null) {
                route = model.getSimpleName();
            }
        }

        public QueryPack build() {
            if (route == null) {
                throw new IllegalArgumentException("Route cannot be null, " +
                        "set class in constructor, or use withRoutingContext or withCustomRoute!");
            }

            QueryPack queryPack = new QueryPack();
            queryPack.query = query;
            queryPack.route = route;
            queryPack.pageToken = pageToken;
            queryPack.requestEtag = requestEtag;
            queryPack.orderByQueue = orderByQueue;
            queryPack.projections = projections;
            queryPack.params = params;
            queryPack.aggregateFunction = aggregateFunction;
            queryPack.indexName = indexName;
            queryPack.limit = limit;
            queryPack.calculateKey();

            return queryPack;
        }

        @Fluent
        public QueryPackBuilder withRoutingContext(RoutingContext routingContext) {
            this.requestEtag = routingContext.request().getHeader("If-None-Match");
            this.pageToken = routingContext.request().getParam("pageToken");
            this.query = routingContext.request().query();
            this.route = routingContext.request().path();

            return this;
        }

        @Fluent
        public QueryPackBuilder withCustomRoute(String route) {
            this.route = route;

            return this;
        }

        @Fluent
        public QueryPackBuilder withCustomQuery(String query) {
            this.query = query;

            return this;
        }

        @Fluent
        public QueryPackBuilder withPageToken(String pageToken) {
            this.pageToken = pageToken;

            return this;
        }

        @Fluent
        public QueryPackBuilder withProjections(String[] projections) {
            this.projections = projections;

            return this;
        }

        @Fluent
        public QueryPackBuilder withRequestEtag(String requestEtag) {
            this.requestEtag = requestEtag;

            return this;
        }

        @Fluent
        public QueryPackBuilder withFilterParameters(Map<String, List<FilterParameter>> params) {
            this.params = params;

            return this;
        }

        @Fluent
        public QueryPackBuilder withOrderByQueue(Queue<OrderByParameter> orderByQueue) {
            this.orderByQueue = orderByQueue;

            return this;
        }

        @Fluent
        public QueryPackBuilder withAggregateFunction(AggregateFunction aggregateFunction) {
            this.aggregateFunction = aggregateFunction;

            return this;
        }

        @Fluent
        public QueryPackBuilder withIndexName(String indexName) {
            this.indexName = indexName;

            return this;
        }

        @Fluent
        public QueryPackBuilder withLimit(Integer limit) {
            this.limit = limit;

            return this;
        }
    }

    private void calculateKey() {
        baseEtagKey = ModelUtils.returnNewEtag(Objects.hashCode(this));
    }

    public String getBaseEtagKey() {
        return baseEtagKey;
    }

    public String getQuery() {
        return query;
    }

    public String getRoute() {
        return route;
    }

    public String getPageToken() {
        return pageToken;
    }

    public String getRequestEtag() {
        return requestEtag;
    }

    public Queue<OrderByParameter> getOrderByQueue() {
        return orderByQueue;
    }

    public Map<String, List<FilterParameter>> getParams() {
        return params;
    }

    public AggregateFunction getAggregateFunction() {
        return aggregateFunction;
    }

    public String[] getProjections() {
        return projections;
    }

    public String getIndexName() {
        return indexName;
    }

    public Integer getLimit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryPack queryPack = (QueryPack) o;

        return Objects.equals(baseEtagKey, queryPack.baseEtagKey);
    }

    @Override
    public int hashCode() {
        final int[] hash = {Objects.hash(query, route, pageToken, params, aggregateFunction, indexName, limit)};

        if (orderByQueue != null) {
            if (orderByQueue.size() > 0) {
                orderByQueue.forEach(o -> hash[0] ^= o.hashCode());
            }
        }

        if (projections != null) {
            hash[0] ^= Arrays.hashCode(projections);
        }

        return hash[0];
    }
}
