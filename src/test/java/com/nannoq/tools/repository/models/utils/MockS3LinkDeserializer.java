package com.nannoq.tools.repository.models.utils;

import com.amazonaws.services.dynamodbv2.datamodeling.S3Link;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/**
 * Created by anders on 16/01/2017.
 */
public class MockS3LinkDeserializer extends StdDeserializer<S3Link> {
    public MockS3LinkDeserializer() {
        super(S3Link.class);
    }

    @Override
    public S3Link deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException {
        return null;
    }
}
