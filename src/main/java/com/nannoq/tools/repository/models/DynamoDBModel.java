package com.nannoq.tools.repository.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.vertx.codegen.annotations.Fluent;

/**
 * Created by anders on 12/09/16.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface DynamoDBModel {
    @JsonIgnore
    String getHash();
    @JsonIgnore
    String getRange();

    @Fluent
    DynamoDBModel setHash(String hash);

    @Fluent
    DynamoDBModel setRange(String range);
}
