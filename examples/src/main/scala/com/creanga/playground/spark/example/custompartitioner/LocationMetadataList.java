package com.creanga.playground.spark.example.custompartitioner;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LocationMetadataList {

    private List<LocationMetadata> list = new ArrayList<>();
    private long size;

    public LocationMetadataList() {
    }

    public void add(LocationMetadata locationMetadata) {
        list.add(locationMetadata);
        size += locationMetadata.getLocation().filesTotalSize;
    }

    public List<LocationMetadata> getList(){
        return ImmutableList.copyOf(list);
        //return Collections.unmodifiableList(list);
    }

    public long getSize() {
        return size;
    }
}
