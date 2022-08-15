package com.creanga.playground.spark.util;

import org.apache.commons.math3.random.RandomGenerator;

/**
 * A fast random number generator based on the simple and fast xor-shift pseudo
 * random number generator (RNG) specified in Marsaglia, George. (2003). Xorshift RNGs.
 */
public class FastRandom extends java.util.Random implements RandomGenerator {
    final long Y = 842502087L, Z = 3579807591L, W = 273326509L;
    long x, y, z, w;

    public FastRandom() {
        seed((int) System.currentTimeMillis());
    }

    @Override
    public void setSeed(int seed) {
        seed(seed);
    }

    @Override
    public void setSeed(int[] seed) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void setSeed(long seed) {
        seed((int) seed);
    }

    public void seed(int seed) {
        x = seed;
        y = Y;
        z = Z;
        w = W;
    }

    @Override
    public void nextBytes(byte[] buffer) {
        // Fill up the bulk of the buffer in chunks of 4 bytes at a time.
        long x = this.x, y = this.y, z = this.z, w = this.w;
        int i = 0;
        long t;
        for (int bound = buffer.length - 3; i < bound; ) {
            t = (x ^ (x << 11));
            x = y;
            y = z;
            z = w;
            w = (w ^ (w >> 19)) ^ (t ^ (t >> 8));

            buffer[i++] = (byte) w;
            buffer[i++] = (byte) (w >> 8);
            buffer[i++] = (byte) (w >> 16);
            buffer[i++] = (byte) (w >> 24);
        }

        if (i < buffer.length) {
            t = (x ^ (x << 11));
            x = y;
            y = z;
            z = w;
            w = (w ^ (w >> 19)) ^ (t ^ (t >> 8));

            buffer[i++] = (byte) w;
            if (i < buffer.length) {
                buffer[i++] = (byte) (w >> 8);
                if (i < buffer.length) {
                    buffer[i++] = (byte) (w >> 16);
                    if (i < buffer.length) {
                        buffer[i] = (byte) (w >> 24);
                    }
                }
            }
        }
        this.x = x;
        this.y = y;
        this.z = z;
        this.w = w;
    }


    @Override
    public long nextLong() {
        x ^= x >>> 12;
        x ^= x << 25;
        return 2685821657736338717L * (x ^= (x >>> 27));
    }

    @Override
    public int nextInt() {
        return (int)nextLong();
    }

    @Override
    public int nextInt(final int n) {
        if (n <= 0) throw new IllegalArgumentException();
        // No special provision for n power of two: all our bits are good.
        return (int)((nextLong() >>> 1) % n);
    }

}
