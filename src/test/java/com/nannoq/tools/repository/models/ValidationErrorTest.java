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

import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

public class ValidationErrorTest {
    private ValidationError validationError;

    @Before
    public void setUp() throws Exception {
        validationError = new ValidationError("Cannot be null!", "viewCount");
    }

    @Test
    public void toJson() throws Exception {
        final JsonObject jsonObject = validationError.toJson();

        assertEquals("Cannot be null!", jsonObject.getString("description"));
        assertEquals("viewCount", jsonObject.getString("fieldName"));
    }

    @Test
    public void validateNotNull() throws Exception {
        assertNotNull(ValidationError.validateNotNull(null, "field"));
        assertNull(ValidationError.validateNotNull("lol", "field"));
    }

    @Test
    public void validateDate() throws Exception {
        assertNotNull(ValidationError.validateDate(null, "field"));
        assertNotNull(ValidationError.validateDate(new Date(1), "field"));
        assertNull(ValidationError.validateDate(new Date(), "field"));
    }

    @Test
    public void validateTextLength() throws Exception {
        assertNotNull(ValidationError.validateTextLength(null, "field", 5));
        assertNotNull(ValidationError.validateTextLength("lol", "field", 2));
        assertNull(ValidationError.validateTextLength("lol", "field", 3));
    }
}