package com.nannoq.tools.repository.models;

import com.nannoq.tools.repository.models.utils.FilterParameterTestClass;
import com.nannoq.tools.repository.utils.FilterParameter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilterParameterTest {
    private FilterParameter<FilterParameterTestClass> validParameter;

    @Before
    public void setUp() throws Exception {
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
            .withKlazz(FilterParameterTestClass.class)
            .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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
        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
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

        validParameter = FilterParameter.<FilterParameterTestClass>builder()
                .withKlazz(FilterParameterTestClass.class)
                .withField("viewCount")
                .withEq(1000)
                .withGt("lol")
                .build();
    }
}