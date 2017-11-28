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

package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.utils.AggregateFunction;
import com.nannoq.tools.repository.utils.GroupingConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static com.nannoq.tools.repository.utils.AggregateFunctions.*;
import static org.junit.Assert.*;

public class AggregateFunctionTest {
    private AggregateFunction validAggregateFunction;

    @Before
    public void setUp() throws Exception {
        validAggregateFunction = new AggregateFunction(COUNT, "viewCount");
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
        assertTrue(new AggregateFunction(MIN, "viewCount").isMin());
    }

    @Test
    public void isMax() throws Exception {
        assertFalse(validAggregateFunction.isMax());
        assertTrue(validAggregateFunction.isCount());
        assertTrue(new AggregateFunction(MAX, "viewCount").isMax());
    }

    @Test
    public void isAverage() throws Exception {
        assertFalse(validAggregateFunction.isAverage());
        assertTrue(validAggregateFunction.isCount());
        assertTrue(new AggregateFunction(AVG, "viewCount").isAverage());
    }

    @Test
    public void isSum() throws Exception {
        assertFalse(validAggregateFunction.isSum());
        assertTrue(validAggregateFunction.isCount());
        assertTrue(new AggregateFunction(SUM, "viewCount").isSum());
    }

    @Test
    public void isCount() throws Exception {
        assertTrue(validAggregateFunction.isCount());
        assertFalse(validAggregateFunction.isMax());
    }

    @Test
    public void getGroupBy() throws Exception {
        assertTrue(validAggregateFunction.getGroupBy().isEmpty());
        assertFalse(new AggregateFunction(COUNT, "viewCount",
                Collections.singletonList(new GroupingConfiguration("viewCount"))).getGroupBy().isEmpty());
    }

    @Test
    public void hasGrouping() throws Exception {
        assertFalse(validAggregateFunction.hasGrouping());
        assertTrue(new AggregateFunction(COUNT, "viewCount",
                Collections.singletonList(new GroupingConfiguration("viewCount"))).hasGrouping());
    }
}