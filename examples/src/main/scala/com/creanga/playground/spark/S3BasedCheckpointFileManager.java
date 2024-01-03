/*
 * Checkpoint File Manager for MinIO (C) 2023 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creanga.playground.spark;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.sql.execution.streaming.CheckpointFileManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class S3BasedCheckpointFileManager implements CheckpointFileManager {

    Path path;
    Configuration hadoopConfiguration;

    String API_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    String SERVER_ENDPOINT = "fs.s3a.endpoint";
    String SERVER_REGION = "fs.s3a.region";

    boolean pathStyleAccess = "true".equals(hadoopConfiguration.get(API_PATH_STYLE_ACCESS, "true"));
    String endpoint = hadoopConfiguration.get(SERVER_ENDPOINT, "http://127.0.0.1:9000");
    String location = hadoopConfiguration.get(SERVER_REGION, "us-east-1");
    private final AmazonS3 s3Client =
            AmazonS3ClientBuilder.standard()
                    .withCredentials(Credentials.load(hadoopConfiguration))
                    .withPathStyleAccessEnabled(pathStyleAccess)
                    .withEndpointConfiguration(new EndpointConfiguration(endpoint, location))
                    .build();

    public S3BasedCheckpointFileManager(Path path, Configuration hadoopConfiguration) {
        this.path = path;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public CancellableFSDataOutputStream createAtomic(Path path, boolean overwriteIfPossible) {

        String p = path.toString().replaceAll("s3a://", "").trim();
        if (!p.isEmpty()) {
            if (p.charAt(0) == Path.SEPARATOR_CHAR) {
                p = p.substring(1);
            }
        }
        int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
        String bucketName = p.substring(0, objectPos);
        String objectName = p.substring(objectPos + 1);
        if (objectName.equals("")){
            throw new IllegalArgumentException(path + " is not a valid path for the file system");
        }
        S3OutputStream outputStream = new S3OutputStream(s3Client, bucketName, objectName);


        return new CancellableFSDataOutputStream(outputStream) {
            @Override
            public void cancel() {
                outputStream.cancel();
            }

            @Override
            public void close() throws IOException {
                outputStream.close();
            }
        };
    }

    @Override
    public FSDataInputStream open(Path path) {
        String p = path.toString().replaceAll("s3a://", "").trim();
        if (!p.isEmpty()) {
            if (p.charAt(0) == Path.SEPARATOR_CHAR) {
                p = p.substring(1);
            }
        }
        int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
        String bucketName = p.substring(0, objectPos);
        String objectName = p.substring(objectPos + 1);
        if (objectName.equals("")){
            throw new IllegalArgumentException(path + " is not a valid path for the file system");
        }
        return new FSDataInputStream(new S3InputStream(s3Client, bucketName, objectName));

    }

    @Override
    public FileStatus[] list(Path path, PathFilter filter) {

        String p = path.toString().replaceAll("s3a://", "").trim();
        if (!p.isEmpty()) {
            if (p.charAt(0) == Path.SEPARATOR_CHAR) {
                p = p.substring(1);
            }
        }

        int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
        String bucketName = p.substring(0, objectPos);
        String prefix = p.substring(objectPos + 1);

        ObjectListing objectsResponse = s3Client.listObjects(bucketName, prefix);
        List<FileStatus> results = new ArrayList<>();

        objectsResponse.getObjectSummaries().forEach(s3ObjectSummary -> results.add(newFile(s3ObjectSummary)));


        while (objectsResponse.isTruncated()) {
            objectsResponse = s3Client.listNextBatchOfObjects(objectsResponse);
            objectsResponse.getObjectSummaries().forEach(s3ObjectSummary -> results.add(newFile(s3ObjectSummary)));
        }

        return (FileStatus[]) results.toArray();
    }

    @Override
    public FileStatus[] list(Path path) {
        return list(path, null);
    }

    private FileStatus newFile(S3ObjectSummary obj) {
        return new FileStatus(obj.getSize(), false, 1, 64 * 1024 * 1024,
                obj.getLastModified().getTime(), new Path(obj.getBucketName(), obj.getKey()));
    }

    @Override
    public void mkdirs(Path path) {
        //none
    }

    @Override
    public boolean exists(Path path) {
        String p = path.toString().replaceAll("s3a://", "").trim();
        if (!p.isEmpty()) {
            if (p.charAt(0) == Path.SEPARATOR_CHAR) {
                p = p.substring(1);
            }
        }
        int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
        String bucketName = p.substring(0, objectPos);
        String objectName = p.substring(objectPos + 1);
        if (objectName.equals("")){
            throw new IllegalArgumentException(path + " is not a valid path for the file system");
        }
        return s3Client.doesObjectExist(bucketName, objectName);
    }

    @Override
    public void delete(Path path) {
        String p = path.toString().replaceAll("s3a://", "").trim();
        if (!p.isEmpty()) {
            if (p.charAt(0) == Path.SEPARATOR_CHAR) {
                p = p.substring(1);
            }
        }
        int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
        String bucketName = p.substring(0, objectPos);
        String objectName = p.substring(objectPos + 1);

        try {
            ObjectMetadata objectMeta = s3Client.getObjectMetadata(bucketName, objectName);

            if (StringUtils.isEmpty(objectMeta.getVersionId())){
                s3Client.deleteVersion(bucketName, objectName, "null");
            }else{
                s3Client.deleteVersion(
                        bucketName,
                        objectName,
                        objectMeta.getVersionId());
            }

        } catch (AmazonS3Exception e){
            if (e.getStatusCode() != 404) {
                throw e;
            }
        }

    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public Path createCheckpointDirectory() {
        return path;
    }
}
