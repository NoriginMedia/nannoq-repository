package com.nannoq.tools.repository.utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nannoq.tools.repository.models.ETagable;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

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
        this.groupBy = new ArrayList<>();
    }

    public AggregateFunction(AggregateFunctions function, String field) {
        this(function, field, new ArrayList<>());
    }

    public AggregateFunction(AggregateFunctions function, String field, List<GroupingConfiguration> groupBy) {
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
}
