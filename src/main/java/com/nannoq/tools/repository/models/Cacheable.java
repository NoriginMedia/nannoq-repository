package com.nannoq.tools.repository.models;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * This class defines an interface for cacheable models.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface Cacheable {
    @DynamoDBIgnore
    @JsonIgnore
    String getCachePartitionKey();
}
