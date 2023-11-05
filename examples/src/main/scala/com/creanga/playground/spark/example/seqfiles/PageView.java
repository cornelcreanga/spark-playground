package com.creanga.playground.spark.example.seqfiles;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class PageView implements Serializable {
    String id;
    String personId;
    long timestamp;
    String url;

    public PageView() {
    }

    public PageView(String id, String personId, long timestamp, String url) {
        this.id = id;
        this.personId = personId;
        this.timestamp = timestamp;
        this.url = url;
    }

    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        id = in.readUTF();
        personId = in.readUTF();
        timestamp = in.readLong();
        url = in.readUTF();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(personId);
        out.writeLong(timestamp);
        out.writeUTF(url);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPersonId() {
        return personId;
    }

    public void setPersonId(String personId) {
        this.personId = personId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
