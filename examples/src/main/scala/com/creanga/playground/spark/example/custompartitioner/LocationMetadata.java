package com.creanga.playground.spark.example.custompartitioner;

import java.io.Serializable;
import java.util.Arrays;

public class LocationMetadata implements Serializable {

    private Location location;
    private String[] files;

    public LocationMetadata(Location location, String[] files) {
        this.location = location;
        this.files = files;
    }

    public Location getLocation() {
        return location;
    }

    public String[] getFiles() {
        return files;
    }

    @Override
    public String toString() {
        return "LocationMetadata{" +
                "location=" + location +
                ", files=" + Arrays.toString(files) +
                '}';
    }
}
