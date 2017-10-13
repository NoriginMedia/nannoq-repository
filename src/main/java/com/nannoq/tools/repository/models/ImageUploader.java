package com.nannoq.tools.repository.models;

import com.amazonaws.services.dynamodbv2.datamodeling.S3Link;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.ext.web.FileUpload;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.FileImageOutputStream;
import javax.imageio.stream.ImageOutputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;

public interface ImageUploader {
    Logger logger = LoggerFactory.getLogger(ImageUploader.class.getSimpleName());

    default void doUpload(Vertx vertx, File file, Supplier<S3Link> s3LinkSupplier, Future<Boolean> fut) {
        vertx.<Boolean>executeBlocking(uploadFuture -> {
            try {
                if (!file.exists()) {
                    uploadFuture.fail(new UnknownError("File does not exist!"));
                } else {

                    File convertedFile = imageToPng(file);
                    S3Link location = s3LinkSupplier.get();
                    location.getAmazonS3Client().putObject(
                            location.getBucketName(), location.getKey(), convertedFile);
                    convertedFile.delete();

                    logger.debug("Content stored for: " + file.getPath());

                    uploadFuture.complete(Boolean.TRUE);
                }
            } catch (Exception e) {
                logger.error("Failure in external storage!", e);

                if (file != null) {
                    file.delete();
                }

                uploadFuture.tryFail(e);
            }
        }, false, contentRes -> {
            if (contentRes.failed()) {
                logger.error("FAILED Storage for: " + file.getPath(), contentRes.cause());

                fut.fail(contentRes.cause());
            } else {
                fut.complete(Boolean.TRUE);
            }
        });
    }

    default void doUpload(Vertx vertx, FileUpload file, Supplier<S3Link> s3LinkSupplier, Future<Boolean> fut) {
        vertx.<Boolean>executeBlocking(uploadFuture -> {
            try {
                File convertedFile = imageToPng(new File(file.uploadedFileName()));
                S3Link location = s3LinkSupplier.get();
                location.getAmazonS3Client().putObject(
                        location.getBucketName(), location.getKey(), convertedFile);
                convertedFile.delete();

                logger.debug("Content stored for: " + file.uploadedFileName());

                uploadFuture.complete(Boolean.TRUE);
            } catch (Exception e) {
                logger.error("Failure in external storage!", e);

                uploadFuture.tryFail(e);
            }
        }, false, contentRes -> {
            if (contentRes.failed()) {
                logger.error("FAILED Storage for: " + file.uploadedFileName(), contentRes.cause());

                fut.fail(contentRes.cause());
            } else {
                fut.complete(Boolean.TRUE);
            }
        });
    }

    default void doUpload(Vertx vertx, String url, Supplier<S3Link> s3LinkSupplier, Future<Boolean> fut) {
        HttpClientOptions options = new HttpClientOptions();
        options.setConnectTimeout(10000);
        options.setSsl(true);

        HttpClientRequest req = vertx.createHttpClient(options).getAbs(url, response -> {
            if (response.statusCode() == 200) {
                String uuid = UUID.randomUUID().toString();
                logger.debug("Response to: " + uuid);

                if (response.statusCode() == 200) {
                    response.pause();

                    final AsyncFile[] asyncFile = new AsyncFile[1];
                    OpenOptions openOptions = new OpenOptions()
                            .setCreate(true)
                            .setWrite(true);

                    response.endHandler(end -> {
                        logger.debug("Reading image...");

                        if (asyncFile[0] != null) {
                            asyncFile[0].flush(res -> asyncFile[0].close(closeRes ->
                                    doUpload(vertx, new File(uuid), s3LinkSupplier, fut)));
                        } else {
                            logger.error("File is missing!");

                            fut.fail("File is missing!");
                        }
                    });

                    vertx.fileSystem().open(uuid, openOptions, file -> {
                        logger.debug("File opened!");

                        if (file.succeeded()) {
                            asyncFile[0] = file.result();
                            Pump pump = Pump.pump(response, asyncFile[0]);
                            pump.start();
                        } else {
                            logger.error("Unable to open file for download!", file.cause());
                        }

                        logger.debug("Read response!");

                        response.resume();
                    });
                } else {
                    logger.error("Error reading external file (" + response.statusCode() + ") for: " + uuid);

                    fut.fail(response.statusMessage());
                }
            } else {
                logger.error("Error reading external file...");

                fut.fail(new UnknownError());
            }
        });

        req.setFollowRedirects(true);
        req.exceptionHandler(fut::fail);
        req.end();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    default File imageToPng(File file) throws IOException {
        file.setReadable(true);
        BufferedImage image = ImageIO.read(file);
        image = convertImageToRGB(image, BufferedImage.TYPE_INT_RGB);
        ImageWriter jpgWriter = ImageIO.getImageWritersByFormatName("jpg").next();
        ImageWriteParam jpgWriteParam = jpgWriter.getDefaultWriteParam();
        jpgWriteParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        jpgWriteParam.setCompressionQuality(1.0f);

        ImageOutputStream outputStream = new FileImageOutputStream(file);
        jpgWriter.setOutput(outputStream);
        IIOImage outputImage = new IIOImage(image, null, null);
        jpgWriter.write(null, outputImage, jpgWriteParam);
        jpgWriter.dispose();

        return file;
    }

    default BufferedImage convertImageToRGB(BufferedImage src, int typeIntRgb) {
        BufferedImage img = new BufferedImage(src.getWidth(), src.getHeight(), typeIntRgb);
        Graphics2D g2d = img.createGraphics();
        g2d.drawImage(src, 0, 0, null);
        g2d.dispose();

        return img;
    }
}
