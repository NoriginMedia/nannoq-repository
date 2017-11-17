package com.nannoq.tools.repository.utils;

import com.nannoq.tools.repository.models.Model;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.annotation.Nonnull;

/**
 * This class defines the operation to be performed on a specific field, with OR and AND types included.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class FilterParameter<E extends Model> {
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
    private Class<E> classType;

    public enum FILTER_TYPE {AND, OR}

    public FilterParameter() {}

    public static <T extends Model> FilterParameterBuilder<T> builder() {
        return new FilterParameterBuilder<>();
    }

    @SuppressWarnings("WeakerAccess")
    public static class FilterParameterBuilder<E extends Model> {
        private static final Logger logger = LoggerFactory.getLogger(FilterParameterBuilder.class.getSimpleName());

        private Class<E> klazz;
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

        private boolean klazzSet = false;
        private boolean fieldSet = false;
        private boolean operatorSet = false;
        private boolean typeSet = false;

        private FilterParameterBuilder() {}

        public FilterParameter<E> build() {
            if (field == null) {
                throw new IllegalArgumentException("Field cannot be null for a filter by parameter!");
            }

            if (klazz == null) {
                throw new IllegalArgumentException("Klazz cannot be null for a filter parameter!");
            }

            FilterParameter<E> param = new FilterParameter<>();
            param.classType = klazz;
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
        @SuppressWarnings("unchecked")
        public FilterParameterBuilder<E> withKlazz(Class<E> klazz) {
            if (klazzSet) {
                throw new IllegalArgumentException("Klazz cannot be replaced after being initially set!");
            }

            klazzSet = true;
            this.klazz = klazz;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withField(String field) {
            if (fieldSet) {
                throw new IllegalArgumentException("Field cannot be replaced after being initially set!");
            }

            fieldSet = true;
            this.field = field;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withEq(Object eq) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.eq = eq;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withNe(Object ne) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.ne = ne;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withGt(Object gt) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.gt = gt;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withGe(Object ge) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.ge = ge;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withLt(Object lt) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.lt = lt;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withLe(Object le) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.le = le;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withBetween(Object gt, Object lt) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.gt = gt;
            this.lt = lt;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withInclusiveBetween(Object ge, Object le) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.ge = ge;
            this.le = le;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withGeLtVariableBetween(Object ge, Object lt) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.ge = ge;
            this.lt = lt;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withLeGtVariableBetween(Object gt, Object le) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.le = le;
            this.gt = gt;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withContains(Object contains) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.contains = contains;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withNotContains(Object notContains) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.notContains = notContains;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withBeginsWith(Object beginsWith) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.beginsWith = beginsWith;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withIn(Object[] in) {
            if (operatorSet) {
                throw new IllegalArgumentException("Operator cannot be replaced after being initially set!");
            }

            operatorSet = true;
            this.in = in;

            return this;
        }

        @Fluent
        public FilterParameterBuilder<E> withType(FILTER_TYPE type) {
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
    public FilterParameter<E> setField(String field) {
        this.field = field;

        return this;
    }

    @Fluent
    public FilterParameter<E> setClassType(Class<E> classType) {
        this.classType = classType;

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
    public FilterParameter<E> setType(@Nonnull String type) {
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

    public Class<E> getClassType() {
        return classType;
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
}
