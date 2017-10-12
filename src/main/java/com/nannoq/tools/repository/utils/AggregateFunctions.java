package com.nannoq.tools.repository.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by anders on 14/01/2017.
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