package com.nannoq.tools.repository.utils;

import java.util.List;

/**
 * This class defines the filterpackfield attached to a specific model.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
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
