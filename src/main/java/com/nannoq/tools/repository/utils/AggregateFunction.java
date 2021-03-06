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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nannoq.tools.repository.models.ETagable;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static com.nannoq.tools.repository.utils.AggregateFunctions.COUNT;
import static java.util.stream.Collectors.toList;

/**
 * This class defines an aggregation function with the field, the function, and any grouping parameters.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class AggregateFunction {
    private AggregateFunctions function;
    private String field;
    private List<GroupingConfiguration> groupBy;

    @JsonIgnore
    private JsonObject validationError;

    public enum TIMEUNIT_DATE { HOUR, TWELVE_HOUR, DAY, WEEK, MONTH, YEAR }

    public AggregateFunction() {
        this.validationError = new JsonObject();
        this.groupBy = new LinkedList<>();
    }

    public static AggregateFunctionBuilder builder() {
        return new AggregateFunctionBuilder();
    }

    @SuppressWarnings("WeakerAccess")
    public static class AggregateFunctionBuilder {
        private static final Logger logger = LoggerFactory.getLogger(FilterParameter.FilterParameterBuilder.class.getSimpleName());

        private AggregateFunctions function;
        private String field;
        private List<GroupingConfiguration> groupBy = new LinkedList<>();

        private AggregateFunctionBuilder() {
        }

        public AggregateFunction build() {
            if (function == null) {
                throw new IllegalArgumentException("Function cannot be null!");
            }

            if (field == null && function != COUNT) {
                throw new IllegalArgumentException("Field cannot be null!");
            }

            AggregateFunction func = new AggregateFunction();
            func.function = function;
            func.field = field;
            func.groupBy = groupBy == null ? new LinkedList<>() : groupBy;

            return func;
        }

        @Fluent
        public AggregateFunctionBuilder withAggregateFunction(AggregateFunctions function) {
            this.function = function;

            return this;
        }

        @Fluent
        public AggregateFunctionBuilder withField(String field) {
            this.field = field;

            return this;
        }

        @Fluent
        public AggregateFunctionBuilder withGroupBy(@Nonnull List<GroupingConfiguration> groupBy) {
            if (groupBy.size() > 3) {
                throw new IllegalArgumentException("You can only group 3 levels deep!");
            }

            if (this.groupBy == null) this.groupBy = new LinkedList<>();
            this.groupBy.addAll(groupBy);

            return this;
        }

        @Fluent
        public AggregateFunctionBuilder addGroupBy(@Nonnull GroupingConfiguration groupBy) {
            if (this.groupBy == null) this.groupBy = new LinkedList<>();

            if (this.groupBy.size() == 3) {
                throw new IllegalArgumentException("You can only group 3 levels deep!");
            }

            this.groupBy.add(groupBy);

            return this;
        }
    }

    public AggregateFunctions getFunction() {
        return function;
    }

    public boolean isMin() {
        return function == AggregateFunctions.MIN;
    }

    public boolean isMax() {
        return function == AggregateFunctions.MAX;
    }

    public boolean isAverage() {
        return function == AggregateFunctions.AVG;
    }

    public boolean isSum() {
        return function == AggregateFunctions.SUM;
    }

    public boolean isCount() {
        return function == COUNT;
    }

    public List<GroupingConfiguration> getGroupBy() {
        return groupBy;
    }

    public boolean hasGrouping() { return !groupBy.isEmpty(); }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public <E extends ETagable> boolean validateFieldForFunction(Class<E> TYPE) {
        if (!isMin() && !isMax() && !isAverage()) return true;

        if (field == null) {
            String errorMessage = "Field name cannot be null...";

            switch (getFunction()) {
                case MIN:
                    validationError.put("min_error", errorMessage);

                    break;
                case MAX:
                    validationError.put("max_error", errorMessage);

                    break;
                case AVG:
                    validationError.put("avg_error", errorMessage);

                    break;
            }

            return false;
        }

        if (hasGrouping()) {
            final List<Boolean> collect = groupBy.stream()
                    .map(groupingConfiguration ->
                            groupingConfiguration.validate(TYPE, field, validationError))
                    .collect(toList());

            return collect.stream().anyMatch(res -> !res);
        }

        return validationError.isEmpty();
    }

    public JsonObject getValidationError() {
        return validationError;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateFunction that = (AggregateFunction) o;

        return function == that.function &&
                Objects.equals(field, that.field) &&
                Objects.equals(groupBy, that.groupBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, field, groupBy);
    }
}
