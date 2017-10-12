package com.nannoq.tools.repository.utils;

import java.util.List;

/**
 * Created by anders on 10/02/2017.
 */
public class FilterPackModel {
    private String model;
    private List<FilterPackField> fields;

    public FilterPackModel() {}

    public String getModel() {
        return model;
    }

    public List<FilterPackField> getFields() {
        return fields;
    }
}
