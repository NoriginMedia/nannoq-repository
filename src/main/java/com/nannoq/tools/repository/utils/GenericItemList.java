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

package com.nannoq.tools.repository.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.nannoq.tools.repository.models.ModelUtils;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * This class defines a generic list for items. Used for aggregation.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@DataObject(generateConverter = true)
public class GenericItemList {
    private String etag;
    private String pageToken;
    private int count;
    private List<JsonObject> items;

    public GenericItemList() {
    }

    public GenericItemList(JsonObject jsonObject) {
        this.etag = jsonObject.getString("etag");
        this.pageToken = jsonObject.getString("pageToken");
        this.count = jsonObject.getInteger("count");
        this.items = jsonObject.getJsonArray("items").stream()
                .map(e -> (JsonObject) e)
                .collect(toList());
    }

    public GenericItemList(String pageToken, int count, List<JsonObject> items) {
        this.pageToken = pageToken;
        this.count = count;
        this.items = items;
        final long[] etagCode = {1234567890L};
        if (items != null) items.forEach(item -> etagCode[0] = etagCode[0] ^ item.encode().hashCode());
        etag = ModelUtils.returnNewEtag(etagCode[0]);
    }

    public JsonObject toJson() {
        return JsonObject.mapFrom(this);
    }

    public JsonObject toJson(String[] projections) {
        JsonObject jsonObject = new JsonObject()
                .put("etag", etag == null ? "NoTag" : etag)
                .put("pageToken", pageToken == null ? "END_OF_LIST" : pageToken)
                .put("count", count);

        JsonArray jsonItems = new JsonArray();

        if (getItems() != null) {
            getItems().forEach(jsonItems::add);
        }

        jsonObject.put("items", jsonItems);

        return jsonObject;
    }

    public String toJsonString() {
        return toJsonString(new String[]{});
    }

    public String toJsonString(@Nonnull String[] projections) {
        return toJson(projections).encode();
    }

    public String getPageToken() {
        return pageToken;
    }

    public void setPageToken(String pageToken) {
        this.pageToken = pageToken;
    }

    public String getEtag() {
        return etag;
    }

    public void setEtag(String etag) {
        this.etag = etag;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public List<JsonObject> getItems() {
        return items;
    }

    public void setItems(List<JsonObject> items) {
        this.items = items;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GenericItemList itemList = (GenericItemList) o;

        if (count != itemList.count) return false;
        if (etag != null ? !etag.equals(itemList.etag) : itemList.etag != null) return false;
        if (pageToken != null ? !pageToken.equals(itemList.pageToken) : itemList.pageToken != null) return false;
        return items != null ? items.equals(itemList.items) : itemList.items == null;
    }

    @Override
    public int hashCode() {
        int result = etag != null ? etag.hashCode() : 0;
        result = 31 * result + (pageToken != null ? pageToken.hashCode() : 0);
        result = 31 * result + count;
        result = 31 * result + (items != null ? items.hashCode() : 0);
        return result;
    }
}
