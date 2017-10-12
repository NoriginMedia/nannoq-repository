package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.utils.OrderByParameter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderByParameterTest {
    private OrderByParameter validParameter;

    @Before
    public void setUp() throws Exception {
        validParameter = OrderByParameter.builder()
                .withField("viewCount")
                .withDirection("asc")
                .build();
    }

    @Test
    public void isAsc() throws Exception {
        assertTrue(validParameter.isAsc());
        assertFalse(validParameter.isDesc());
    }

    @Test
    public void isDesc() throws Exception {
        validParameter.setDirection("desc");
        assertTrue(validParameter.isDesc());
        assertFalse(validParameter.isAsc());
    }

    @Test
    public void isValid() throws Exception {
        assertTrue(validParameter.isValid());
        validParameter.setField(null);
        assertFalse(validParameter.isValid());
    }
}