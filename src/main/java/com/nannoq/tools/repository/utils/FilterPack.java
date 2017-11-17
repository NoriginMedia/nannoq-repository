package com.nannoq.tools.repository.utils;

import java.util.List;

/**
 * This class defines a package of multiple filterPackModels.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
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
