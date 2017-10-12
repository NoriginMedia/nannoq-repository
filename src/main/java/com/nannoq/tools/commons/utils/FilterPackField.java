package com.nannoq.tools.repository.utils;

import java.util.List;

/**
 * Created by anders on 10/02/2017.
 */
public class FilterPackField {
    private String field;
    private List<FilterParameter> parameters;

    public FilterPackField() {}

    public String getField() {
        return field;
    }

    public List<FilterParameter> getParameters() {
        return parameters;
    }
}
