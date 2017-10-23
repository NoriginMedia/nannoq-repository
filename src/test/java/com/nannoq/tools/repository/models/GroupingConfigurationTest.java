package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.utils.GroupingConfiguration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupingConfigurationTest {
    private GroupingConfiguration validGroupingConfiguration;

    @Before
    public void setUp() throws Exception {
        validGroupingConfiguration = new GroupingConfiguration("releaseDate");
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
        validGroupingConfiguration = new GroupingConfiguration(
                "releaseDate", "DATE", "WEEK");
        assertTrue(validGroupingConfiguration.hasGroupRanging());
    }
}