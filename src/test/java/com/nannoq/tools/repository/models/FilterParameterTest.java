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

import com.nannoq.tools.repository.models.utils.FilterParameterTestClass;
import com.nannoq.tools.repository.utils.FilterParameter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilterParameterTest {
    private FilterParameter validParameter;

    @Before
    public void setUp() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withEq(1000)
                .build();
    }

    @Test
    public void isEq() throws Exception {
        assertTrue(validParameter.isEq());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isNe() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withNe(1000)
                .build();
        assertTrue(validParameter.isNe());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isIn() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withIn(new Object[] {1000})
                .build();
        assertTrue(validParameter.isIn());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isGt() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
            .withGt(1000)
            .build();
        assertTrue(validParameter.isGt());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isLt() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withLt(1000)
                .build();
        assertTrue(validParameter.isLt());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isGe() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withGe(1000)
                .build();
        assertTrue(validParameter.isGe());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isLe() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withLe(1000)
                .build();
        assertTrue(validParameter.isLe());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isBetween() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withBetween(1000, 2000)
                .build();
        assertTrue(validParameter.isBetween());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isInclusiveBetween() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withInclusiveBetween(1000, 2000)
                .build();
        assertTrue(validParameter.isInclusiveBetween());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isGeLtVariableBetween() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withGeLtVariableBetween(1000, 2000)
                .build();
        assertTrue(validParameter.isGeLtVariableBetween());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isLeGtVariableBetween() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withLeGtVariableBetween(1000, 2000)
                .build();
        assertTrue(validParameter.isLeGtVariableBetween());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isContains() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withContains(1000)
                .build();
        assertTrue(validParameter.isContains());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isNotContains() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withNotContains(1000)
                .build();
        assertTrue(validParameter.isNotContains());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isIn());
        assertFalse(validParameter.isBeginsWith());
    }

    @Test
    public void isBeginsWith() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withBeginsWith(1000)
                .build();
        assertTrue(validParameter.isBeginsWith());
        assertTrue(validParameter.isValid());
        assertFalse(validParameter.isEq());
        assertFalse(validParameter.isNe());
        assertFalse(validParameter.isGt());
        assertFalse(validParameter.isLt());
        assertFalse(validParameter.isGe());
        assertFalse(validParameter.isLe());
        assertFalse(validParameter.isBetween());
        assertFalse(validParameter.isInclusiveBetween());
        assertFalse(validParameter.isGeLtVariableBetween());
        assertFalse(validParameter.isLeGtVariableBetween());
        assertFalse(validParameter.isContains());
        assertFalse(validParameter.isNotContains());
        assertFalse(validParameter.isIn());
    }

    @Test(expected = IllegalArgumentException.class)
    public void isValid() throws Exception {
        assertTrue(validParameter.isValid());

        validParameter = FilterParameter.<FilterParameterTestClass>builder("viewCount")
                .withEq(1000)
                .withGt("lol")
                .build();
    }
}