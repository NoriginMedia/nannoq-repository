package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.utils.CrossModelAggregateFunction;
import com.nannoq.tools.repository.utils.CrossModelGroupingConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static com.nannoq.tools.repository.utils.AggregateFunctions.*;
import static org.junit.Assert.*;

public class CrossModelAggregateFunctionTest {
    private CrossModelAggregateFunction validAggregateFunction;

    @Before
    public void setUp() throws Exception {
        validAggregateFunction = new CrossModelAggregateFunction(COUNT, "viewCount");
    }

    @Test
    public void getFunction() throws Exception {
        assertEquals(COUNT, validAggregateFunction.getFunction());
        assertNotEquals(MIN, validAggregateFunction.getFunction());
        assertNotEquals(MAX, validAggregateFunction.getFunction());
        assertNotEquals(AVG, validAggregateFunction.getFunction());
        assertNotEquals(SUM, validAggregateFunction.getFunction());
    }

    @Test
    public void isMin() throws Exception {
        assertFalse(validAggregateFunction.isMin());
        assertTrue(validAggregateFunction.isCount());
        assertTrue(new CrossModelAggregateFunction(MIN, "viewCount").isMin());
    }

    @Test
    public void isMax() throws Exception {
        assertFalse(validAggregateFunction.isMax());
        assertTrue(validAggregateFunction.isCount());
        assertTrue(new CrossModelAggregateFunction(MAX, "viewCount").isMax());
    }

    @Test
    public void isAverage() throws Exception {
        assertFalse(validAggregateFunction.isAverage());
        assertTrue(validAggregateFunction.isCount());
        assertTrue(new CrossModelAggregateFunction(AVG, "viewCount").isAverage());
    }

    @Test
    public void isSum() throws Exception {
        assertFalse(validAggregateFunction.isSum());
        assertTrue(validAggregateFunction.isCount());
        assertTrue(new CrossModelAggregateFunction(SUM, "viewCount").isSum());
    }

    @Test
    public void isCount() throws Exception {
        assertTrue(validAggregateFunction.isCount());
        assertFalse(validAggregateFunction.isMax());
    }

    @Test
    public void getGroupBy() throws Exception {
        assertTrue(validAggregateFunction.getGroupBy().isEmpty());
        assertTrue(new CrossModelAggregateFunction(COUNT, "viewCount",
                Collections.singletonList(new CrossModelGroupingConfiguration(Collections.singletonList("viewCount")))).hasGrouping());
    }

    @Test
    public void hasGrouping() throws Exception {
        assertFalse(validAggregateFunction.hasGrouping());
        assertTrue(new CrossModelAggregateFunction(COUNT, "viewCount",
                Collections.singletonList(new CrossModelGroupingConfiguration(Collections.singletonList("viewCount")))).hasGrouping());
    }
}