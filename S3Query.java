package com.games24x7.pn;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.SelectObjectContentEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.amazonaws.util.IOUtils.copy;

public class S3Query {
    private static final Logger logger = LoggerFactory.getLogger(
            MethodHandles.lookup().lookupClass());
    private static AmazonS3 s3Client;

    public static void s3Client() {
        s3Client = AmazonS3ClientBuilder.defaultClient();
    }

    public static void queryS3(File outputFileName, SelectObjectContentRequest request) {
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);
        try (OutputStream fileOutputStream = new FileOutputStream(outputFileName, true);
             SelectObjectContentResult result = s3Client.selectObjectContent(request)) {
            InputStream resultInputStream = result.getPayload().getRecordsInputStream(
                    new SelectObjectContentEventVisitor() {
                        @Override
                        public void visit(SelectObjectContentEvent.StatsEvent event) {
                            logger.info(
                                    "Received Stats, Bytes Scanned: " + event.getDetails().getBytesScanned()
                                            + " Bytes Processed: " + event.getDetails().getBytesProcessed());
                        }

                        @Override
                        public void visit(SelectObjectContentEvent.EndEvent event) {
                            isResultComplete.set(true);
                            logger.info("Received End Event. Result is complete.");
                        }
                    }
            );
            copy(resultInputStream, fileOutputStream);
            if (!isResultComplete.get()) {
                throw new RuntimeException(
                        "S3 Select request was incomplete as End Event was not received.");
            }
            logger.info("content copied, query filtered file name:{}", outputFileName);
        } catch (Exception e) {
            logger.error("Exception in queryS3 request:{}", e);
            throw new RuntimeException(e);
        }
    }
}
