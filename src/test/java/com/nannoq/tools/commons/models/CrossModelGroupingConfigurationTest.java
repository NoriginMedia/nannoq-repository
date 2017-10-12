package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.utils.CrossModelGroupingConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CrossModelGroupingConfigurationTest {
    private CrossModelGroupingConfiguration validGroupingConfiguration;

    @Before
    public void setUp() throws Exception {
        validGroupingConfiguration = new CrossModelGroupingConfiguration(Collections.singletonList("releaseDate"));
    }

    @Test
    public void isFullList() throws Exception {
        assertFalse(validGroupingConfiguration.isFullList());
        validGroupingConfiguration.setFullList(true);
        assertTrue(validGroupingConfiguration.isFullList());
    }

    @Test
    public void hasGroupRanging() throws Exception {
        assertFalse(validGroupingConfiguration.hasGroupRanging());
        validGroupingConfiguration = new CrossModelGroupingConfiguration(
                Collections.singletonList("releaseDate"), "DATE", "WEEK");
        assertTrue(validGroupingConfiguration.hasGroupRanging());
    }
}