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

import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.utils.ItemList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * This class defines a container for the result of an index operation.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class ItemListResult<K extends Model> {
    private String etagBase;
    private int count;
    private List<K> items;
    private String pageToken;
    private String[] projections;
    private ItemList<K> itemList;

    private boolean cacheHit;
    private long preOperationProcessingTime;
    private long operationProcessingTime;
    private long postOperationProcessingTime;

    public ItemListResult(String etagBase, int count, @Nonnull List<K> items, @Nonnull String pageToken,
                          @Nonnull String[] projections, boolean cacheHit) {
        this.etagBase = etagBase;
        this.count = count;
        this.items = items;
        this.pageToken = pageToken;
        this.projections = projections;
        this.cacheHit = cacheHit;
    }

    public ItemListResult(ItemList<K> itemList, boolean cacheHit) {
        this.itemList = itemList;
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
        if (itemList != null) {
            return itemList;
        } else {
            return new ItemList<>(etagBase, pageToken, count, items, projections);
        }
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
