package com.nannoq.tools.repository.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.vertx.codegen.annotations.Fluent;

/**
 * This class defines the interface for models that interact with the DynamoDBRepository.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
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
