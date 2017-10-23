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