package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.utils.AggregateFunctions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AggregateFunctionsTest {
    @Test
    public void forValue() throws Exception {
        assertEquals(AggregateFunctions.MIN, AggregateFunctions.forValue("min"));
        assertEquals(AggregateFunctions.MIN, AggregateFunctions.forValue("MIN"));
        assertEquals(AggregateFunctions.MAX, AggregateFunctions.forValue("max"));
        assertEquals(AggregateFunctions.MAX, AggregateFunctions.forValue("MAX"));
        assertEquals(AggregateFunctions.AVG, AggregateFunctions.forValue("avg"));
        assertEquals(AggregateFunctions.AVG, AggregateFunctions.forValue("AVG"));
        assertEquals(AggregateFunctions.SUM, AggregateFunctions.forValue("sum"));
        assertEquals(AggregateFunctions.SUM, AggregateFunctions.forValue("SUM"));
        assertEquals(AggregateFunctions.COUNT, AggregateFunctions.forValue("count"));
        assertEquals(AggregateFunctions.COUNT, AggregateFunctions.forValue("COUNT"));
        assertNull(AggregateFunctions.forValue("bogus"));
    }

    @Test
    public void toValue() throws Exception {
        assertEquals("MIN", AggregateFunctions.MIN.toValue());
        assertEquals("MAX", AggregateFunctions.MAX.toValue());
        assertEquals("AVG", AggregateFunctions.AVG.toValue());
        assertEquals("SUM", AggregateFunctions.SUM.toValue());
        assertEquals("COUNT", AggregateFunctions.COUNT.toValue());
    }
}