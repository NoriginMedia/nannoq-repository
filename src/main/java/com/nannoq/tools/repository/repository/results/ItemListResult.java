package com.nannoq.tools.repository.repository.results;

import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.utils.ItemList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * User: anders
 * Date: 13.11.17 15:43
 */
public class ItemListResult<K extends ETagable & Model> {
    private int count;
    private List<K> items;
    private String pageToken;
    private String[] projections;

    private boolean cacheHit;
    private long preOperationProcessingTime;
    private long operationProcessingTime;
    private long postOperationProcessingTime;

    public ItemListResult(int count, @Nonnull List<K> items, @Nonnull String pageToken, @Nonnull String[] projections, boolean cacheHit) {
        this.count = count;
        this.items = items;
        this.pageToken = pageToken;
        this.projections = projections;
        this.cacheHit = cacheHit;
    }

    public ItemListResult(ItemList<K> itemList, @Nonnull String[] projections, boolean cacheHit) {
        this.items = itemList.getItems();
        this.count = itemList.getCount();
        this.pageToken = itemList.getPageToken();
        this.projections = projections;
        this.cacheHit = cacheHit;
    }

    public void setCacheHit(boolean cacheHit) {
        this.cacheHit = cacheHit;
    }

    public boolean isCacheHit() {
        return cacheHit;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public List<K> getItems() {
        return items;
    }

    public void setItems(List<K> items) {
        this.items = items;
    }

    public String getPageToken() {
        return pageToken;
    }

    public void setPageToken(String pageToken) {
        this.pageToken = pageToken;
    }

    public String[] getProjections() {
        return projections;
    }

    public void setProjections(String[] projections) {
        this.projections = projections;
    }

    public ItemList<K> getItemList() {
        return new ItemList<>(pageToken, count, items, projections);
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
