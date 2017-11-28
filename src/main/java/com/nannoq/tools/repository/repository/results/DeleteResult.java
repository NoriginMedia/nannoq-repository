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

package com.nannoq.tools.repository.repository.results;

import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;

/**
 * This class defines a container for the result of a Delete operation.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class DeleteResult<K extends ETagable & Model> {
    private final K item;
    private long preOperationProcessingTime;
    private long operationProcessingTime;
    private long postOperationProcessingTime;

    public DeleteResult(K item) {
        this.item = item;
    }

    public K getItem() {
        return item;
    }

    public long getPreOperationProcessingTime() {
        return preOperationProcessingTime;
    }

    public void setPreOperationProcessingTime(long preOperationProcessingTime) {
        this.preOperationProcessingTime = preOperationProcessingTime;
    }

    public long getOperationProcessingTime() {
        return operationProcessingTime;
    }

    public void setOperationProcessingTime(long operationProcessingTime) {
        this.operationProcessingTime = operationProcessingTime;
    }

    public long getPostOperationProcessingTime() {
        return postOperationProcessingTime;
    }

    public void setPostOperationProcessingTime(long postOperationProcessingTime) {
        this.postOperationProcessingTime = postOperationProcessingTime;
    }
}
