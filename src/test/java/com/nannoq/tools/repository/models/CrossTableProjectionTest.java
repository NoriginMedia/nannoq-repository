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