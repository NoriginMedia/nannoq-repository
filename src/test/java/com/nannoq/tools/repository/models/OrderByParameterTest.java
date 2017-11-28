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