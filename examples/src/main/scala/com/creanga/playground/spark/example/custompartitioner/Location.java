package com.creanga.playground.spark.example.custompartitioner;

import java.io.Serializable;

public class Location implements Serializable {

    String batchId;
    String id;
    long filesTotalSize;

    public Location(String batchId, String id, long filesTotalSize) {
        this.batchId = batchId;
        this.id = id;
        this.filesTotalSize = filesTotalSize;
    }

    public String getBatchId() {
        return batchId;
    }

    public String getId() {
        return id;
    }

    public long getFilesTotalSize() {
        return filesTotalSize;
    }

    @Override
    public String toString() {
        return "Location{" +
                "batchId='" + batchId + '\'' +
                ", cid='" + id + '\'' +
                ", filesTotalSize=" + filesTotalSize +
                '}';
    }
}
