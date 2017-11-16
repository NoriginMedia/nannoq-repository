package com.nannoq.tools.repository.repository.results;

import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;

/**
 * User: anders
 * Date: 13.11.17 20:59
 */
public class UpdateResult<K extends ETagable & Model> {
    private final K item;
    private long preOperationProcessingTime;
    private long operationProcessingTime;
    private long postOperationProcessingTime;

    public UpdateResult(K item) {
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
