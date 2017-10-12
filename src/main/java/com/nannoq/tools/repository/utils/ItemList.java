package com.nannoq.tools.repository.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.models.ModelUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Created by anders on 06/09/16.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ItemList<E extends ETagable & Model> {
    private String etag;
    private String pageToken;
    private int count;
    private List<E> items;

    public ItemList() {
    }

    public ItemList(String pageToken, int count, List<E> items, @Nonnull String[] projections) {
        this.pageToken = pageToken;
        this.count = count;
        this.items = items;
        final long[] etagCode = {1234567890L};
        if (items != null) items.forEach(item -> etagCode[0] = etagCode[0] ^ item.toJsonFormat(projections).encode().hashCode());
        etag = ModelUtils.returnNewEtag(etagCode[0]);
    }

    public JsonObject toJson(String[] projections) {
        JsonObject jsonObject = new JsonObject()
                .put("etag", etag == null ? "NoTag" : etag)
                .put("pageToken", pageToken == null ? "END_OF_LIST" : pageToken)
                .put("count", count);

        JsonArray jsonItems = new JsonArray();

        if (getItems() != null) {
            getItems().stream()
                    .map(m -> m.toJsonFormat(projections))
                    .forEach(jsonItems::add);
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

    public List<E> getItems() {
        return items;
    }

    public void setItems(List<E> items) {
        this.items = items;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ItemList<?> itemList = (ItemList<?>) o;

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
