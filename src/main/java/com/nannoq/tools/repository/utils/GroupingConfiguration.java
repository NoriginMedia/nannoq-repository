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
import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.Objects;

/**
 * This class defines the grouping configuration for a single model, similar to the cross-model grouping configurations.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class GroupingConfiguration {
    private String groupBy;
    private String groupByUnit = "";
    private Object groupByRange;
    private String groupingSortOrder = "desc";
    private int groupingListLimit = 10;
    @JsonIgnore
    private boolean fullList;

    public GroupingConfiguration() {
    }

    public static GroupingConfigurationBuilder builder() {
        return new GroupingConfigurationBuilder();
    }

    public static class GroupingConfigurationBuilder {
        private static final Logger logger = LoggerFactory.getLogger(GroupingConfigurationBuilder.class.getSimpleName());

        private String groupBy;
        private String groupByUnit = "";
        private Object groupByRange;
        private String groupingSortOrder = "desc";
        private int groupingListLimit = 10;
        private boolean fullList;

        private GroupingConfigurationBuilder() {}

        public GroupingConfiguration build() {
            if (groupBy == null) {
                throw new IllegalArgumentException("Field cannot be null for a filter by parameter!");
            }

            GroupingConfiguration param = new GroupingConfiguration();
            param.groupBy = groupBy;
            param.groupByUnit = groupByUnit;
            param.groupByRange = groupByRange;
            param.groupingSortOrder = groupingSortOrder;
            param.groupingListLimit = groupingListLimit;
            param.fullList = fullList;

            return param;
        }

        @Fluent
        public GroupingConfigurationBuilder withGroupBy(String groupBy) {
            this.groupBy = groupBy;

            return this;
        }

        @Fluent
        public GroupingConfigurationBuilder withGroupByUnit(String groupByUnit) {
            this.groupByUnit = groupByUnit == null ? "" : groupByUnit;

            return this;
        }
        @Fluent
        public GroupingConfigurationBuilder withGroupByRange(Object groupByRange) {
            this.groupByRange = groupByRange;

            return this;
        }
        @Fluent
        public GroupingConfigurationBuilder withGroupingSortOrder(String groupingSortOrder) {
            this.groupingSortOrder = groupingSortOrder == null ? "desc" : groupingSortOrder;

            return this;
        }
        @Fluent
        public GroupingConfigurationBuilder withGroupingListLimit(int groupingListLimit) {
            this.groupingListLimit = groupingListLimit;

            return this;
        }
        @Fluent
        public GroupingConfigurationBuilder withFullList(boolean fullList) {
            this.fullList = fullList;

            return this;
        }
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

    public boolean hasGroupRanging() { return !groupByUnit.equalsIgnoreCase(""); }

    public boolean validate(Class TYPE, String fieldName, JsonObject validationError) {
        if (groupingListLimit == 0) {
            groupingListLimit = Integer.MAX_VALUE;
            fullList = true;
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

        return groupingListLimit == that.groupingListLimit &&
                fullList == that.fullList &&
                Objects.equals(groupBy, that.groupBy) &&
                Objects.equals(groupByUnit, that.groupByUnit) &&
                Objects.equals(groupByRange, that.groupByRange) &&
                Objects.equals(groupingSortOrder, that.groupingSortOrder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupBy, groupByUnit, groupByRange == null ? 1234L : groupByRange.toString(),
                groupingSortOrder, fullList, groupingListLimit);
    }
}
