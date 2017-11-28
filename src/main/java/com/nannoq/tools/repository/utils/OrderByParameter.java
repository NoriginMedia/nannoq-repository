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

import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * This class defines an orderByParameter, which is used for sorting results.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class OrderByParameter {
    private String field;
    private String direction;

    public OrderByParameter() {}

    public static OrderByParameter.OrderByParameterBuilder builder() {
        return new OrderByParameter.OrderByParameterBuilder();
    }

    @SuppressWarnings("WeakerAccess")
    public static class OrderByParameterBuilder {
        private static final Logger logger = LoggerFactory.getLogger(FilterParameter.FilterParameterBuilder.class.getSimpleName());

        private String field;
        private String direction;

        private OrderByParameterBuilder() {
        }

        public OrderByParameter build() {
            if (field == null) {
                throw new IllegalArgumentException("Field cannot be null for an order by parameter!");
            }

            OrderByParameter param = new OrderByParameter();
            param.field = field;

            if (direction != null) {
                param.direction = direction;
            } else {
                param.direction = "desc";
            }

            return param;
        }

        @Fluent
        @SuppressWarnings("unchecked")
        public OrderByParameter.OrderByParameterBuilder withField(String field) {
            this.field = field;
            return this;
        }

        @Fluent
        public OrderByParameter.OrderByParameterBuilder withDirection(String direction) {
            this.direction = direction;

            return this;
        }
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public boolean isAsc() {
        return direction != null && direction.equalsIgnoreCase("asc");
    }

    public boolean isDesc() {
        return direction == null || direction.equalsIgnoreCase("desc");
    }

    public boolean isValid() {
        return field != null && ((isAsc() && !isDesc()) || (isDesc() && !isAsc()));
    }
}
