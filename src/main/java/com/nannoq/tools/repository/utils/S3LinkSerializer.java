package com.nannoq.tools.repository.utils;

import com.amazonaws.services.dynamodbv2.datamodeling.S3Link;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * Created by anders on 07/12/2016.
 */
public class S3LinkSerializer extends StdSerializer<S3Link> {
    public S3LinkSerializer() {
        super(S3Link.class);
    }

    @Override
    public void serialize(S3Link s3Link, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectFieldStart("s3");
        jsonGenerator.writeStringField("bucket", s3Link.getBucketName());
        jsonGenerator.writeStringField("key", s3Link.getKey());
        jsonGenerator.writeStringField("region", s3Link.getS3Region().getFirstRegionId());
        jsonGenerator.writeEndObject();
        jsonGenerator.writeEndObject();
    }
}
