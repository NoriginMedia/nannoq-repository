package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.utils.AggregateFunctions;
import com.nannoq.tools.repository.utils.CrossTableProjection;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CrossTableProjectionTest {
    @Test
    public void validate() throws Exception {
        assertFalse(new CrossTableProjection().validate(AggregateFunctions.COUNT).isEmpty());
        assertFalse(new CrossTableProjection().validate(AggregateFunctions.MIN).isEmpty());

        assertTrue(new CrossTableProjection(Collections.singletonList("feedItems"), Collections.singletonList("feedItems")).validate(AggregateFunctions.COUNT).isEmpty());
        assertFalse(new CrossTableProjection().validate(AggregateFunctions.MIN).isEmpty());
    }
}