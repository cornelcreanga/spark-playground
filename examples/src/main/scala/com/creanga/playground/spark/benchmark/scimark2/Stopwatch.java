package com.creanga.playground.spark.benchmark.scimark2;

/**
 * Provides a stopwatch to measure elapsed time.
 * <p>
 * Example of use:
 * <pre>
 * Stopwatch Q = new Stopwatch;
 * Q.start();
 * //
 * // code to be timed here ...
 * //
 * Q.stop();
 * System.out.println("elapsed time was: " + Q.read() + " seconds.");
 * </pre>
 *
 * @author Roldan Pozo
 * @version 14 October 1997, revised 1999-04-24
 */
public class Stopwatch {
    private boolean running;
    private double last_time;
    private double total;


    public Stopwatch() {
        reset();
    }

    /**
     * Return system time (in seconds)
     */
    public static double seconds() {
        return (System.currentTimeMillis() * 0.001);
    }

    /**
     * Return system time (in seconds)
     */
    public void reset() {
        running = false;
        last_time = 0.0;
        total = 0.0;
    }

    /**
     * Start (and reset) timer
     */
    public void start() {
        if (!running) {
            running = true;
            total = 0.0;
            last_time = seconds();
        }
    }

    /**
     * Resume timing, after stopping.  (Does not wipe out
     * accumulated times.)
     */
    public void resume() {
        if (!running) {
            last_time = seconds();
            running = true;
        }
    }


    /**
     * Stop timer
     */
    public double stop() {
        if (running) {
            total += seconds() - last_time;
            running = false;
        }
        return total;
    }


    /**
     * Display the elapsed time (in seconds)
     */
    public double read() {
        if (running) {
            total += seconds() - last_time;
            last_time = seconds();
        }
        return total;
    }
}



