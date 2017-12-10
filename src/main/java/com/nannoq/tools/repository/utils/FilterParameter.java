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

import com.nannoq.tools.repository.models.Model;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Objects;

/**
 * This class defines the operation to be performed on a specific field, with OR and AND types included.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class FilterParameter {
    private String field;
    private Object eq;
    private Object ne;
    private Object gt;
    private Object ge;
    private Object lt;
    private Object le;
    private Object contains;
    private Object notContains;
    private Object beginsWith;
    private Object[] in;
    private String type;

    public enum FILTER_TYPE {AND, OR}

    public FilterParameter() {}

    public static FilterParameterBuilder builder() {
        return new FilterParameterBuilder();
    }

    public static FilterParameterBuilder builder(String field) {
        return new FilterParameterBuilder(field);
    }

    @SuppressWarnings("WeakerAccess")
    public static class FilterParameterBuilder {
        private static final Logger logger = LoggerFactory.getLogger(FilterParameterBuilder.class.getSimpleName());

        private String field;

        private Object eq;
        private Object ne;
        private Object gt;
        private Object ge;
        private Object lt;
        private Object le;
        private Object contains;
        private Object notContains;
        private Object beginsWith;
        private Object[] in;
        private String type;

        private boolean fieldSet = false;
        private boolean operatorSet = false;
        private boolean typeSet = false;

        private FilterParameterBuilder() {}

        private FilterParameterBuilder(String field) {
            withField(field);
        }

        public FilterParameter build() {
            if (field == null) {
                throw new IllegalArgumentException("Field cannot be null for a filter by parameter!");
            }

            FilterParameter param = new FilterParameter();
            param.field = field;
            param.eq = eq;
            param.ne = ne;
            param.gt = gt;
            param.ge = ge;
            param.lt = lt;
            param.le = le;
            param.contains = contains;
            param.notContains = notContains;
            param.beginsWith = beginsWith;
            param.in = in;
            param.type = type;

            if (!param.isValid()) {
                JsonObject errors = new JsonObject();
                param.collectErrors(errors);
                        
                throw new IllegalArgumentException("This parameter is invalid!" + errors.encodePrettily());
            }

            return param;
        }

        @Fluent
        public FilterParameterBuilder withField(String field) {
            if (fieldSet) {
                throw new IllegalArgumentException("Field cannot be replaced after being initially set!");
            }

            fieldSet = true;
            this.field = field;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withEq(Object eq) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.eq = eq;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withNe(Object ne) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.ne = ne;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withGt(Object gt) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.gt = gt;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withGe(Object ge) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.ge = ge;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withLt(Object lt) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.lt = lt;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withLe(Object le) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.le = le;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withBetween(Object gt, Object lt) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.gt = gt;
            this.lt = lt;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withInclusiveBetween(Object ge, Object le) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.ge = ge;
            this.le = le;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withGeLtVariableBetween(Object ge, Object lt) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.ge = ge;
            this.lt = lt;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withLeGtVariableBetween(Object gt, Object le) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.le = le;
            this.gt = gt;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withContains(Object contains) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.contains = contains;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withNotContains(Object notContains) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.notContains = notContains;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withBeginsWith(Object beginsWith) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.beginsWith = beginsWith;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withIn(Object[] in) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.in = in;

            return this;
        }

        @Fluent
        public FilterParameterBuilder withType(FILTER_TYPE type) {
            if (typeSet) {
                throw new IllegalArgumentException("Type cannot be replaced after being initially set!");
            }

            typeSet = true;

            switch (type) {
                case AND:
                    this.type = "and";

                    break;
                case OR:
                    this.type = "or";

                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized type!");
            }

            return this;
        }
    }

    @Fluent
    public FilterParameter setField(String field) {
        this.field = field;

        return this;
    }

    public String getField() {
        return field;
    }

    public Object getEq() {
        return eq;
    }

    public Object getGt() {
        return gt;
    }

    public Object getLt() {
        return lt;
    }

    public Object getNe() {
        return ne;
    }

    public Object getGe() {
        return ge;
    }

    public Object getLe() {
        return le;
    }

    public Object getContains() {
        return contains;
    }

    public Object getNotContains() {
        return notContains;
    }

    public Object getBeginsWith() {
        return beginsWith;
    }

    public Object[] getIn() {
        return in;
    }

    public String getType() {
        if (type == null) {
            return "AND";
        }

        if (type.equalsIgnoreCase("AND") || type.equalsIgnoreCase("OR")) {
            return type.toUpperCase();
        }

        return null;
    }

    @Fluent
    public FilterParameter setType(@Nonnull String type) {
        switch (FILTER_TYPE.valueOf(type.toUpperCase())) {
            case AND:
                this.type = "AND";

                break;
            case OR:
                this.type = "OR";

                break;
            default:
                throw new IllegalArgumentException("Unrecognized type!");
        }

        return this;
    }

    public boolean isEq() {
        return eq != null && ne == null && gt == null && lt == null && ge == null && le == null && contains == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isNe() {
        return eq == null && ne != null && gt == null && lt == null && ge == null && le == null && contains == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isIn() {
        return in != null && eq == null && ne == null && gt == null && lt == null && ge == null && le == null && contains == null && notContains == null && beginsWith == null;
    }

    public boolean isGt() {
        return gt != null && lt == null && ge == null && le == null && eq == null && ne == null && contains == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isLt() {
        return lt != null && gt == null && ge == null && le == null && eq == null && ne == null && contains == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isGe() {
        return ge != null && le == null && gt == null && lt == null && eq == null && ne == null && contains == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isLe() {
        return le != null && ge == null && gt == null && lt == null && eq == null && ne == null && contains == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isBetween() {
        return gt != null && lt != null && ge == null && le == null && eq == null && ne == null && contains == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isInclusiveBetween() {
        return ge != null && le != null && gt == null && lt == null && eq == null && ne == null && contains == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isGeLtVariableBetween() {
        return ge != null && lt != null && gt == null && le == null && eq == null && ne == null && contains == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isLeGtVariableBetween() {
        return le != null && gt != null && ge == null && lt == null && eq == null && ne == null && contains == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isContains() {
        return contains != null && le == null && ge == null && gt == null && lt == null && eq == null && ne == null && notContains == null && beginsWith == null && in == null;
    }

    public boolean isNotContains() {
        return notContains != null && le == null && ge == null && gt == null && lt == null && eq == null && ne == null && contains == null && beginsWith == null && in == null;
    }

    public boolean isBeginsWith() {
        return beginsWith != null && le == null && ge == null && gt == null && lt == null && eq == null && ne == null && contains == null && notContains == null && in == null;
    }

    public boolean isIllegalRangedKeyParam() {
        return isContains() || isNotContains() || isIn();
    }

    public boolean isValid() {
        Logger logger = LoggerFactory.getLogger(FilterParameter.class);
        logger.debug("EQ: " + isEq());
        logger.debug("NE: " + isNe());
        logger.debug("IN: " + isIn());
        logger.debug("GT: " + isGt());
        logger.debug("LT: " + isLt());
        logger.debug("GE: " + isGe());
        logger.debug("LE: " + isLe());
        logger.debug("BETWEEN: " + isBetween());
        logger.debug("INCLUSIVE_BETWEEN: " + isInclusiveBetween());
        logger.debug("GE_LT_VARIABLE_BETWEEN: " + isGeLtVariableBetween());
        logger.debug("LE_GT_VARIABLE_BETWEEN: " + isLeGtVariableBetween());
        logger.debug("CONTAINS: " + isContains());
        logger.debug("NOT_CONTAINS: " + isNotContains());
        logger.debug("BEGINS_WITH: " + isBeginsWith());

        return field != null && getType() != null &&
                (isEq() || isNe() || isGt() || isLt() || isGe() || isLe() ||
                        isBetween() || isInclusiveBetween() || isGeLtVariableBetween() || isLeGtVariableBetween() ||
                        isContains() || isNotContains() || isBeginsWith() || isIn());
    }

    public void collectErrors(JsonObject errors) {
        if ((ne != null || eq != null) && (gt != null || ge != null || lt != null || le != null)) {
            errors.put(field + "_error", "Filter Parameter error on: '" + field + "', " +
                    "'eq' or 'ne' cannot co exist with 'gt','lt','ge' or 'le' parameters!");
        } else {
            errors.put(field + "_error", "Advanced functions cannot be used in conjunction with simple functions.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilterParameter that = (FilterParameter) o;

        return Objects.equals(field, that.field) &&
                Objects.equals(eq, that.eq) &&
                Objects.equals(ne, that.ne) &&
                Objects.equals(gt, that.gt) &&
                Objects.equals(ge, that.ge) &&
                Objects.equals(lt, that.lt) &&
                Objects.equals(le, that.le) &&
                Objects.equals(contains, that.contains) &&
                Objects.equals(notContains, that.notContains) &&
                Objects.equals(beginsWith, that.beginsWith) &&
                Arrays.equals(in, that.in) &&
                Objects.equals(getType(), that.getType());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(field,
                eq == null ? 1234L : eq.toString(),
                ne == null ? 1234L : ne.toString(),
                gt == null ? 1234L : gt.toString(),
                ge == null ? 1234L : ge.toString(),
                lt == null ? 1234L : lt.toString(),
                le == null ? 1234L : le.toString(),
                contains == null ? 1234L : contains.toString(),
                notContains == null ? 1234L : notContains.toString(),
                beginsWith == null ? 1234L : beginsWith.toString(), getType());
        result = 31 * result + Arrays.hashCode(in);

        return result;
    }
}
