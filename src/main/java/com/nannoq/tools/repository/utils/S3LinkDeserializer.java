/*
 * MIT License
 *
 * Copyright (c) 2017 Anders Mikkelsen
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

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
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.json.JsonObject;

import java.io.IOException;

/**
 * This class defines an deserializer for Jackson for the S3Link class.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class S3LinkDeserializer extends StdDeserializer<S3Link> {
    private static S3ClientCache clientCache;

    public S3LinkDeserializer(@Nullable JsonObject config) {
        super(S3Link.class);

        if (clientCache == null) {
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
