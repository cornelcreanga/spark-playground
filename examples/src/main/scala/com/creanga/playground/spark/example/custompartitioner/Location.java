package com.creanga.playground.spark.example.custompartitioner;

import java.io.Serializable;

public class Location  implements Serializable {

    String batchId;
    String cid;
    long filesTotalSize;

    public Location(String batchId, String cid, long filesTotalSize) {
        this.batchId = batchId;
        this.cid = cid;
        this.filesTotalSize = filesTotalSize;
    }

    public String getBatchId() {
        return batchId;
    }

    public String getCid() {
        return cid;
    }

    public long getFilesTotalSize() {
        return filesTotalSize;
    }

    @Override
    public String toString() {
        return "Location{" +
                "batchId='" + batchId + '\'' +
                ", cid='" + cid + '\'' +
                ", filesTotalSize=" + filesTotalSize +
                '}';
    }
}
