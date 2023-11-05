package com.creanga.playground.spark.util;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UUIDUtils {

    public static byte[] asBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

}
