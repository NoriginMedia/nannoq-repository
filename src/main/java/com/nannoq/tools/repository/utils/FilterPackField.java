package com.nannoq.tools.repository.utils;

import java.util.List;

/**
 * This class defines the parameters attached to a specific field of a model.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
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
