package com.nannoq.tools.repository.utils;

import java.util.List;

/**
 * Created by anders on 10/02/2017.
 */
public class FilterPack {
    private List<FilterPackModel> models;

    public FilterPack() {}

    public List<FilterPackModel> getModels() {
        return models;
    }

    public boolean validate() {
        return true;
    }
}
