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

package com.nannoq.tools.repository.models.utils;

import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.models.ValidationError;
import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import java.util.Date;
import java.util.List;

public class FilterParameterTestClass implements Model {
    private Long viewCount;

    @Override
    public Model setIdentifiers(JsonObject identifiers) {
        return null;
    }

    @Override
    public Model setModifiables(Model newObject) {
        return null;
    }

    @Override
    public Model sanitize() {
        return null;
    }

    @Override
    public List<ValidationError> validateCreate() {
        return null;
    }

    @Override
    public List<ValidationError> validateUpdate() {
        return null;
    }

    @Override
    public Date getCreatedAt() {
        return null;
    }

    @Override
    public Model setCreatedAt(Date date) {
        return null;
    }

    @Override
    public Date getUpdatedAt() {
        return null;
    }

    @Override
    public Model setUpdatedAt(Date date) {
        return null;
    }

    @Override
    public Model setInitialValues(Model record) {
        return null;
    }

    @Override
    public JsonObject toJsonFormat(@Nonnull String[] projections) {
        return null;
    }
}
