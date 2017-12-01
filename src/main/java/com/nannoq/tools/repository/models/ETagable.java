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

import com.fasterxml.jackson.annotation.JsonInclude;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class defines an interface for models that operate on etags.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface ETagable {
    Logger logger = LoggerFactory.getLogger(ETagable.class.getSimpleName());

    String getEtag();
    ETagable setEtag(String etag);

    default Map<String, String> generateAndSetEtag(Map<String, String> map) {
        String oldTag = getEtag();
        AtomicLong etagCode = new AtomicLong(Long.MAX_VALUE);

        Arrays.stream(getClass().getDeclaredFields()).forEach(field -> {
            field.setAccessible(true);

            if (Collection.class.isAssignableFrom(field.getType())) {
                try {
                    Object object = field.get(this);

                    if (object != null) {
                        //noinspection unchecked
                        ((Collection) object).forEach(o -> {
                            final ETagable e = ((ETagable) o);
                            final Map<String, String> stringStringMap = e.generateAndSetEtag(map);
                            map.putAll(stringStringMap);

                            long innerEtagCode = e.getEtag() != null ? e.getEtag().hashCode() : 12345L;

                            etagCode.set(etagCode.get() ^ innerEtagCode);
                        });
                    }
                } catch (IllegalAccessException e) {
                    logger.error("Cannot access collection for etag!", e);
                }
            } else {
                try {
                    final Object value = field.get(this);
                    final long innerEtagCode = value != null ? value.hashCode() : 12345L;

                    etagCode.set(etagCode.get() ^ innerEtagCode);
                } catch (IllegalAccessException e) {
                    logger.error("Cannot access field for etag!", e);
                }
            }
        });

        String newTag = ModelUtils.returnNewEtag(etagCode.get());

        if (oldTag != null && oldTag.equals(newTag)) return map;
        setEtag(newTag);
        map.put(generateEtagKeyIdentifier(), newTag);

        return reGenerateParent(map);
    }

    default Map<String, String> reGenerateParent(Map<String, String> map) {
        return map;
    }

    String generateEtagKeyIdentifier();
}
