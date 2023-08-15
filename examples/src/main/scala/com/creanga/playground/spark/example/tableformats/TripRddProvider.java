package com.creanga.playground.spark.example.tableformats;

import com.creanga.playground.spark.util.FastRandom;
import com.creanga.playground.spark.util.RDDGenerator;
import com.creanga.playground.spark.util.RecordGenerator;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

public class TripRddProvider implements Serializable {

    private final transient JavaSparkContext jsc;
    private final int partitions;
    private final int values;
    private final Map<String, Object> context;
    private final Broadcast<List<UUID>> driversBroadcast;
    private final static FastRandom fastRandom = new FastRandom();

    public static final double initLatitude = 44.481230877283856d;
    public static final double initLongitude = 26.1745387998047d;

    public TripRddProvider(JavaSparkContext jsc, int partitions, int values, Map<String, Object> context,List<UUID> drivers) {
        this.jsc = jsc;
        this.partitions = partitions;
        this.values = values;
        this.context = context;
        driversBroadcast = jsc.broadcast(drivers);
    }

    public JavaRDD<Trip> buildRdd() {

        RecordGenerator<Trip> recordGenerator = (context, itemNumber) -> {
            FastRandom fastRandom = new FastRandom();
            List<UUID> drivers = driversBroadcast.getValue();
            List<Trip> trip = new ArrayList<>();
            for (int i = 0; i < itemNumber; i++) {
                UUID cid = drivers.get(fastRandom.nextInt(drivers.size()-1));
                trip.add(buildTrip(cid.toString()));
            }
            return trip;
        };

        Class clazz = ConsumerRecord.class;
        RDDGenerator<Trip> rdd = RDDGenerator.of(jsc.sc(),
                partitions,
                values,
                context,
                recordGenerator,
                clazz
        );

        return rdd.toJavaRDD();
    }

    private static Trip buildTrip(String driverId) {


        String userId = new UUID(fastRandom.nextLong(),fastRandom.nextLong()).toString();
        String tripId = new UUID(fastRandom.nextLong(),fastRandom.nextLong()).toString();

        double startLatitude = initLatitude + fastRandom.uniform(0, 0.001);
        double startLongitude = initLongitude + fastRandom.uniform(0, 0.001);
        double endLatitude = initLatitude + fastRandom.uniform(0, 0.001);
        double endLongitude = initLongitude + fastRandom.uniform(0, 0.001);

        long orderTimestamp =  daysBack(System.currentTimeMillis(), fastRandom.nextInt(0, 7));
        long startTimestamp = minutesAfter(orderTimestamp, fastRandom.nextInt(0, 10));
        long endTimestamp = minutesAfter(startTimestamp, fastRandom.nextInt(0, 120));
        byte paymentType = fastRandom.nextBoolean() ? (byte) 1 : (byte) 2;

        return new Trip(tripId, userId, driverId, startLatitude, startLongitude, endLatitude, endLongitude, orderTimestamp, startTimestamp, endTimestamp, paymentType);
    }

    public static long daysBack(long timestamp, long days) {
        return timestamp - Duration.ofDays(days).toMillis();
    }

    public static long minutesAfter(long timestamp, long minutes) {
        return timestamp + Duration.ofMinutes(minutes).toMillis();
    }
}
