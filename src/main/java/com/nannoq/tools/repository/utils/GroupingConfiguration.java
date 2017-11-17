package com.nannoq.tools.repository.utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.vertx.core.json.JsonObject;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Date;

/**
 * This class defines the grouping configuration for a single model, similar to the cross-model grouping configurations.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class GroupingConfiguration {
    private String groupBy;
    private String groupByUnit;
    private Object groupByRange;
    private String groupingSortOrder;
    private int groupingListLimit;
    @JsonIgnore
    private boolean fullList;

    public GroupingConfiguration() {
        this.groupBy = null;
        this.groupByUnit = "";
        this.groupByRange = null;
        this.groupingSortOrder = "desc";
        this.groupingListLimit = 10;
    }

    public GroupingConfiguration(String groupBy) {
        this(groupBy, "");
    }

    public GroupingConfiguration(String groupBy, String groupByUnit) {
        this(groupBy, groupByUnit, null);
    }

    public GroupingConfiguration(String groupBy, String groupByUnit, Object groupByRange) {
        this(groupBy, groupByUnit, groupByRange, "desc");
    }

    public GroupingConfiguration(String groupBy, String groupByUnit, Object groupByRange, String groupingSortOrder) {
        this(groupBy, groupByUnit, groupByRange, groupingSortOrder, 10);
    }

    public GroupingConfiguration(String groupBy, String groupByUnit, Object groupByRange, String groupingSortOrder, int groupingListLimit) {
        this(groupBy, groupByUnit, groupByRange, groupingSortOrder, groupingListLimit, false);
    }

    public GroupingConfiguration(String groupBy, String groupByUnit, Object groupByRange, String groupingSortOrder, int groupingListLimit, boolean fullList) {
        this.groupBy = groupBy;
        this.groupByUnit = groupByUnit;
        this.groupByRange = groupByRange;
        this.groupingSortOrder = groupingSortOrder;
        this.groupingListLimit = groupingListLimit;
        this.fullList = fullList;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public String getGroupByUnit() {
        return groupByUnit;
    }

    public Object getGroupByRange() {
        return groupByRange;
    }

    public String getGroupingSortOrder() {
        return groupingSortOrder;
    }

    public int getGroupingListLimit() {
        return groupingListLimit == 0 || isFullList() ? Integer.MAX_VALUE : groupingListLimit;
    }

    public boolean isFullList() {
        return fullList;
    }

    public void setFullList(boolean fullList) {
        this.fullList = fullList;
    }

    public boolean hasGroupRanging() { return !groupByUnit.equalsIgnoreCase(""); }

    public boolean validate(Class TYPE, String fieldName, JsonObject validationError) {
        if (groupingListLimit == 0) {
            groupingListLimit = Integer.MAX_VALUE;
            setFullList(true);
        } else if (groupingListLimit > 100 || groupingListLimit < 1) {
            validationError.put("groupingListLimit",
                    groupBy + ": Must be an Integer between inclusive 1 and inclusive 100! " +
                            "If you are looking for a full list, set the size to 0!");
        }

        if (!(groupingSortOrder.equalsIgnoreCase("asc") ||
                groupingSortOrder.equalsIgnoreCase("desc"))) {
            validationError.put("groupSortOrder",
                    groupBy + ": Only ASC or DESC may be chosen for sorting order!");
        }

        try {
            Field field = TYPE.getDeclaredField(fieldName);
            Type fieldType = field.getType();

            if (fieldType == Long.class || fieldType == Integer.class ||
                    fieldType == Double.class || fieldType == Float.class ||
                    fieldType == Short.class ||
                    fieldType == long.class || fieldType == int.class ||
                    fieldType == double.class || fieldType == float.class ||
                    fieldType == short.class) {
                if (groupByUnit == null) {
                    return true;
                } else if (groupByUnit.equalsIgnoreCase("INTEGER")) {
                    return true;
                }

                return false;
            } else if (fieldType == Date.class) {
                if (groupByUnit == null) {
                    throw new IllegalArgumentException("Cannot aggregate on dates without a unit!");
                } else {
                    try {
                        AggregateFunction.TIMEUNIT_DATE.valueOf(groupByRange.toString().toUpperCase());

                        return true;
                    } catch(IllegalArgumentException ex) {
                        return false;
                    }
                }
            } else {
                throw new IllegalArgumentException("Not an aggregatable field!");
            }
        } catch (IllegalArgumentException iae) {
            validationError.put("field_error",
                    "This field is not of a type that can be aggregated with this function!");
        } catch (NoSuchFieldException e) {
            validationError.put("field_error",
                    "The requested field does not exist on this model...");
        }

        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupingConfiguration that = (GroupingConfiguration) o;

        return groupingListLimit == that.groupingListLimit && groupBy.equals(that.groupBy) &&
                (groupByUnit != null ? groupByUnit.equals(that.groupByUnit) : that.groupByUnit == null) &&
                (groupByRange != null ? groupByRange.equals(that.groupByRange) : that.groupByRange == null) &&
                (groupingSortOrder != null ? groupingSortOrder.equals(that.groupingSortOrder) : that.groupingSortOrder == null);
    }

    @Override
    public int hashCode() {
        int result = groupBy.hashCode();
        result = 31 * result + (groupByUnit != null ? groupByUnit.hashCode() : 0);
        result = 31 * result + (groupByRange != null ? groupByRange.hashCode() : 0);
        result = 31 * result + (groupingSortOrder != null ? groupingSortOrder.hashCode() : 0);
        result = 31 * result + groupingListLimit;

        return result;
    }
}
