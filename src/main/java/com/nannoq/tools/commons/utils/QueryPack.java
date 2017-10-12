package com.nannoq.tools.repository.utils;

import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.models.ModelUtils;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Created by anders on 02/01/2017.
 */
public class QueryPack<E extends ETagable & Model> {
    private String query;
    private String baseEtagKey;
    private String route;
    private String requestEtag;
    private Queue<OrderByParameter> orderByQueue;
    private Map<String, List<FilterParameter<E>>> params;
    private AggregateFunction aggregateFunction;
    private String indexName;
    private Integer limit;

    private QueryPack() {}

    public static <T extends ETagable & Model> QueryPackBuilder<T> builder() {
        return new QueryPackBuilder<>();
    }

    public static class QueryPackBuilder<E extends ETagable & Model> {
        private static Logger logger = LoggerFactory.getLogger(QueryPackBuilder.class.getSimpleName());

        private String query;
        private String baseEtagKey;
        private String route;
        private String requestEtag;
        private Queue<OrderByParameter> orderByQueue;
        private Map<String, List<FilterParameter<E>>> params;
        private AggregateFunction aggregateFunction;
        private String indexName;
        private Integer limit;

        private QueryPackBuilder() {}

        public QueryPack<E> build() {
            baseEtagKey = ModelUtils.returnNewEtag(
                    query == null ? route.hashCode() : route.hashCode() + query.hashCode());

            QueryPack<E> queryPack = new QueryPack<>();
            queryPack.query = query;
            queryPack.baseEtagKey = baseEtagKey;
            queryPack.route = route;
            queryPack.requestEtag = requestEtag;
            queryPack.orderByQueue = orderByQueue;
            queryPack.params = params;
            queryPack.aggregateFunction = aggregateFunction;
            queryPack.indexName = indexName;
            queryPack.limit = limit;

            return queryPack;
        }

        @Fluent
        public QueryPackBuilder<E> withRoutingContext(RoutingContext routingContext) {
            this.requestEtag = routingContext.request().getHeader("If-None-Match");
            this.query = routingContext.request().query();
            this.route = routingContext.request().path();

            return this;
        }

        @Fluent
        public QueryPackBuilder<E> withQuery(String query) {
            this.query = query;

            return this;
        }

        @Fluent
        public QueryPackBuilder<E> withRoute(String route) {
            this.route = route;
            if (route == null) this.route = "NoRoute";

            return this;
        }

        @Fluent
        public QueryPackBuilder<E> withRequestEtag(String requestEtag) {
            this.requestEtag = requestEtag;

            return this;
        }

        @Fluent
        public QueryPackBuilder<E> withFilterParameters(Map<String, List<FilterParameter<E>>> params) {
            this.params = params;

            return this;
        }

        @Fluent
        public QueryPackBuilder<E> withOrderByQueue(Queue<OrderByParameter> orderByQueue) {
            this.orderByQueue = orderByQueue;

            return this;
        }

        @Fluent
        public QueryPackBuilder<E> withAggregateFunction(AggregateFunction aggregateFunction) {
            this.aggregateFunction = aggregateFunction;

            return this;
        }

        @Fluent
        public QueryPackBuilder<E> withIndexName(String indexName) {
            this.indexName = indexName;

            return this;
        }

        @Fluent
        public QueryPackBuilder<E> withLimit(Integer limit) {
            this.limit = limit;

            return this;
        }
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

    public String getRequestEtag() {
        return requestEtag;
    }

    public Queue<OrderByParameter> getOrderByQueue() {
        return orderByQueue;
    }

    public Map<String, List<FilterParameter<E>>> getParams() {
        return params;
    }

    public AggregateFunction getAggregateFunction() {
        return aggregateFunction;
    }

    public String getIndexName() {
        return indexName;
    }

    public Integer getLimit() {
        return limit;
    }
}
