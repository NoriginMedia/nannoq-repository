package com.nannoq.tools.repository.utils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.S3ClientCache;
import com.amazonaws.services.dynamodbv2.datamodeling.S3Link;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.IOException;

/**
 * Created by anders on 07/12/2016.
 */
public class S3LinkDeserializer extends StdDeserializer<S3Link> {
    private static S3ClientCache clientCache;

    public S3LinkDeserializer() {
        super(S3Link.class);

        if (clientCache == null) {
            JsonObject config = Vertx.currentContext() == null ? null : Vertx.currentContext().config();
            String endPoint;

            if (config == null) {
                endPoint = "http://localhost:8001";
            } else {
                endPoint = config.getString("dynamo_endpoint");
            }

            AmazonDynamoDBAsyncClient dynamoDBAsyncClient = new AmazonDynamoDBAsyncClient(
                    new DefaultAWSCredentialsProviderChain()).withEndpoint(endPoint);

            DynamoDBMapper mapper = new DynamoDBMapper(dynamoDBAsyncClient, new DefaultAWSCredentialsProviderChain());

            clientCache = mapper.getS3ClientCache();
        }
    }

    @Override
    public S3Link deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException {
        return S3Link.fromJson(clientCache, jsonParser.readValueAsTree().toString());
    }
}
