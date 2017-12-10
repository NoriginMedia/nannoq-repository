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
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

/**
 * This class defines an aggregation function with the field, the function, and any grouping parameters for using with
 * multiple models.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class CrossModelAggregateFunction {
    private AggregateFunctions function;
    private String field;
    private List<CrossModelGroupingConfiguration> groupBy;

    @JsonIgnore
    private JsonObject validationError;

    private enum TIMEUNIT_DATE { HOUR, TWELVE_HOUR, DAY, WEEK, MONTH, YEAR }

    public CrossModelAggregateFunction() {
        this.validationError = new JsonObject();
        this.groupBy = new ArrayList<>();
    }

    public CrossModelAggregateFunction(AggregateFunctions function, String field) {
        this(function, field, new ArrayList<>());
    }

    public CrossModelAggregateFunction(AggregateFunctions function, String field, List<CrossModelGroupingConfiguration> groupBy) {
        this.function = function;
        this.field = field;
        this.groupBy = groupBy == null ? new ArrayList<>() : groupBy;
        this.validationError = new JsonObject();
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
        return function == AggregateFunctions.COUNT;
    }

    public boolean hasGrouping() { return groupBy != null && groupBy.size() > 0; }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public List<CrossModelGroupingConfiguration> getGroupBy() {
        return groupBy;
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
        CrossModelAggregateFunction that = (CrossModelAggregateFunction) o;

        return function == that.function &&
                Objects.equals(field, that.field) &&
                Objects.equals(groupBy, that.groupBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, field, groupBy);
    }
}
