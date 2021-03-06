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

package com.nannoq.tools.repository.models;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.S3Link;
import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import org.apache.http.HttpHeaders;

import java.io.File;

/**
 * This class defines an interface for models that store content.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public interface CachedContent {
    Logger logger = LoggerFactory.getLogger(CachedContent.class.getSimpleName());

    S3Link getContentLocation();
    void setContentLocation(S3Link s3Link);

    default void storeContent(Vertx vertx, String urlToContent, String bucketName, String bucketPath,
                              Handler<AsyncResult<Boolean>> resultHandler) {
        storeContent(vertx, 0, urlToContent, bucketName, bucketPath, resultHandler);
    }

    default void storeContent(Vertx vertx, int attempt, String urlToContent, String bucketName, String bucketPath,
                              Handler<AsyncResult<Boolean>> resultHandler) {
        long startTime = System.currentTimeMillis();

        HttpClientOptions opts = new HttpClientOptions()
                .setConnectTimeout(10000)
                .setMaxRedirects(100);
        final HttpClient httpClient = vertx.createHttpClient(opts);

        doRequest(vertx, httpClient, attempt, urlToContent, bucketName, bucketPath, result -> {
            if (result.failed()) {
                if (System.currentTimeMillis() < startTime + (60000L * 15L)) {
                    if (attempt < 30) {
                        logger.warn("Failed on: " + urlToContent + " at attempt " + attempt + " ::: " +
                                result.cause().getMessage());

                        vertx.setTimer((attempt == 0 ? 1 : attempt) * 1000L, aLong ->
                                storeContent(vertx, attempt + 1, urlToContent, bucketName, bucketPath, resultHandler));
                    } else {
                        logger.error("Complete failure on: " + urlToContent + " after " + attempt + " attempts!");

                        httpClient.close();
                        resultHandler.handle(result);
                    }
                } else {
                    logger.error("Timeout failure (15 mins) on: " + urlToContent + " after " + attempt + " attempts!");

                    httpClient.close();
                    resultHandler.handle(result);
                }
            } else {
                if (attempt == 0) {
                    logger.debug("Succeed on: " + urlToContent + " at attempt " + attempt);
                } else {
                    logger.warn("Back to normal on: " + urlToContent + " at attempt " + attempt);
                }

                httpClient.close();
                resultHandler.handle(result);
            }
        });
    }

    default void doRequest(Vertx vertx, HttpClient httpClient, int attempt, String urlToContent, String bucketName,
                           String bucketPath, Handler<AsyncResult<Boolean>> resultHandler) {
        DynamoDBMapper dynamoDBMapper = DynamoDBRepository.getS3DynamoDbMapper();
        setContentLocation(DynamoDBRepository.createS3Link(dynamoDBMapper, bucketName, bucketPath));
        final boolean[] finished = {false};

        try {
            HttpClientRequest req = httpClient.getAbs(urlToContent, response -> {
                logger.debug("Response to: " + urlToContent);

                if (response.statusCode() == 200) {
                    response.pause();

                    final AsyncFile[] asyncFile = new AsyncFile[1];
                    OpenOptions openOptions = new OpenOptions()
                            .setCreate(true)
                            .setWrite(true);

                    vertx.fileSystem().open("" + ModelUtils.returnNewEtag(bucketPath.hashCode()), openOptions, file -> {
                        if (file.succeeded()) {
                            asyncFile[0] = file.result();
                            Pump pump = Pump.pump(response, asyncFile[0]);
                            pump.start();
                            response.resume();
                        } else {
                            logger.error("Unable to open file for download!", file.cause());
                        }

                        finished[0] = true;
                    });

                        response.endHandler(end -> {
                            if (asyncFile[0] != null) {
                                asyncFile[0].flush(res -> asyncFile[0].close(closeRes -> vertx.<Boolean>executeBlocking(future -> {
                                    File file = null;

                                    try {
                                        file = new File("" + ModelUtils.returnNewEtag(bucketPath.hashCode()));
                                        S3Link location = getContentLocation();
                                        getContentLocation().getAmazonS3Client().putObject(location.getBucketName(), location.getKey(), file);
                                        file.delete();

                                        logger.debug("Content stored for: " + urlToContent + ", attempt: " + attempt);

                                        future.complete(Boolean.TRUE);
                                    } catch (Exception e) {
                                        logger.error("Failure in external storage!", e);

                                        if (file != null) {
                                            file.delete();
                                        }

                                        future.tryFail(e);
                                    }
                                }, false, contentRes -> {
                                    if (contentRes.failed()) {
                                        logger.error("FAILED Storage for: " + urlToContent + ", attempt: " + attempt,
                                                contentRes.cause());

                                        resultHandler.handle(Future.failedFuture(contentRes.cause()));
                                    } else {
                                        resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                                    }
                                })));
                            }

                            finished[0] = true;
                        });
                } else {
                    finished[0] = true;

                    logger.error("Error reading external file (" + response.statusCode() + ") for: " +
                            urlToContent + ", attempt: " + attempt);

                    resultHandler.handle(Future.failedFuture(response.statusMessage()));
                }
            }).exceptionHandler(e -> {
                finished[0] = true;

                resultHandler.handle(Future.failedFuture(e));
            });

            req.putHeader(HttpHeaders.ACCEPT, "application/octet-stream");
            req.setChunked(true);
            req.setFollowRedirects(true);
            req.end();

            vertx.setTimer(60000L * 10L, time -> {
                if (!finished[0]) {
                    logger.error("Content has been downloading for 10 mins, killing connection...");

                    req.connection().close();
                }
            });

            logger.debug("Fetching: " + urlToContent);
        } catch (Exception e) {
            logger.fatal("Critical error in content storage!", e);
        }
    }
}
