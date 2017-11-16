package com.nannoq.tools.repository.repository.results;

import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;

/**
 * User: anders
 * Date: 13.11.17 15:42
 */
public class ItemResult<K extends ETagable & Model> {
    private final K item;
    private final boolean cacheHit;
    private long preOperationProcessingTime;
    private long operationProcessingTime;
    private long postOperationProcessingTime;

    public ItemResult(K item, boolean cacheHit) {
        this.item = item;
        this.cacheHit = cacheHit;
    }

    public K getItem() {
        return item;
    }

    public boolean isCacheHit() {
        return cacheHit;
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
