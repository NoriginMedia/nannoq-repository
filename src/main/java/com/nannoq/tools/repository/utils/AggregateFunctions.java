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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * This class defines aggregation functions.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public enum AggregateFunctions {
    MIN,
    MAX,
    AVG,
    SUM,
    COUNT;

    private static Map<String, AggregateFunctions> namesMap = new HashMap<String, AggregateFunctions>(5);

    static {
        namesMap.put("MIN", MIN);
        namesMap.put("MAX", MAX);
        namesMap.put("AVG", AVG);
        namesMap.put("SUM", SUM);
        namesMap.put("COUNT", COUNT);
    }

    @JsonCreator
    public static AggregateFunctions forValue(@Nonnull String value) {
        return namesMap.get(StringUtils.upperCase(value));
    }

    @JsonValue
    public String toValue() {
        for (Map.Entry<String, AggregateFunctions> entry : namesMap.entrySet()) {
            if (entry.getValue() == this)
                return entry.getKey();
        }

        return null;
    }
}