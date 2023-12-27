package com.creanga.playground.spark.example.custompartitioner;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.BiConsumer;

public class TestEdgeCase {

//    public static void main(String[] args) throws IOException {
//        int p = 116;
//        String[] numbers = Files.asByteSource(new File("c:\\users\\ruxij\\desktop\\numbers.txt")).asCharSource(Charset.defaultCharset()).read().split(",");
//        double[] customersWeights = new double[numbers.length];
//        for (int i = 0; i < customersWeights.length; i++) {
//            customersWeights[i] = Double.parseDouble(numbers[i]);
//        }
//        String[] uuids = Files.asByteSource(new File("c:\\users\\ruxij\\desktop\\uuids.txt")).asCharSource(Charset.defaultCharset()).read().split(",");
//        List<String> customersUUIDs = new ArrayList<>();
//        for (String uuid : uuids) {
//            customersUUIDs.add(uuid.trim());
//        }
//
//        double[] partitionAvailabilities = new double[p];
//        Arrays.fill(partitionAvailabilities, 1);
//        Map<String, List<PartitionInfo>> customerPartitionProbabilities = new HashMap<>();
//        for (int i = 0; i < customersWeights.length; i++) {
//            double customerPartitionWeight = customersWeights[i];
//            List<PartitionInfo> list = new ArrayList<>();
//            for (int j = 0; j < partitionAvailabilities.length; j++) {
//                if (customerPartitionWeight == 0)
//                    break;
//
//                double partitionAvailability = partitionAvailabilities[j];
//                if (partitionAvailability == 0)
//                    continue;
//                if (customerPartitionWeight >= partitionAvailability) {
//                    list.add(new PartitionInfo(j, partitionAvailability));
//                    partitionAvailabilities[j] = 0;
//                    customerPartitionWeight -= partitionAvailability;
//                } else {
//                    list.add(new PartitionInfo(j, customerPartitionWeight));
//                    partitionAvailabilities[j] -= customerPartitionWeight;
//                    customerPartitionWeight = 0;
//
//                }
//            }
//            customerPartitionProbabilities.put(customersUUIDs.get(i), list);
//        }
//
//        System.out.println(customerPartitionProbabilities.get("10f47ff7-258b-418e-b2f5-65bddc793120"));
//        System.out.println("done");
//
//    }

    public static void main(String[] args) throws IOException {
        int p = 116;
        String[] numbers = Files.asByteSource(new File("c:\\users\\ruxij\\desktop\\numbers.txt")).asCharSource(Charset.defaultCharset()).read().split(",");
        BigDecimal[] customersWeights = new BigDecimal[numbers.length];
        for (int i = 0; i < customersWeights.length; i++) {
            customersWeights[i] = new BigDecimal(Double.parseDouble(numbers[i]));
        }
        String[] uuids = Files.asByteSource(new File("c:\\users\\ruxij\\desktop\\uuids.txt")).asCharSource(Charset.defaultCharset()).read().split(",");
        List<String> customersUUIDs = new ArrayList<>();
        for (String uuid : uuids) {
            customersUUIDs.add(uuid.trim());
        }

        BigDecimal[] partitionAvailabilities = new BigDecimal[p];
        Arrays.fill(partitionAvailabilities, new BigDecimal(1));
        Map<String, List<PartitionInfo>> customerPartitionProbabilities = new HashMap<>();
        for (int i = 0; i < customersWeights.length; i++) {
            BigDecimal customerPartitionWeight = customersWeights[i];
            List<PartitionInfo> list = new ArrayList<>();
            for (int j = 0; j < partitionAvailabilities.length; j++) {
                if (customerPartitionWeight.doubleValue()==0)
                    break;

                BigDecimal partitionAvailability = partitionAvailabilities[j];
                if (partitionAvailability.doubleValue() == 0)
                    continue;
                if (customerPartitionWeight.compareTo(partitionAvailability)>=0) {
                    list.add(new PartitionInfo(j, partitionAvailability.doubleValue()));
                    partitionAvailabilities[j] = new BigDecimal(0);
                    //customerPartitionWeight -= partitionAvailability;
                    customerPartitionWeight = customerPartitionWeight.subtract(partitionAvailability);
                } else {
                    list.add(new PartitionInfo(j, customerPartitionWeight.doubleValue()));
                    //partitionAvailabilities[j] -= customerPartitionWeight;
                    partitionAvailabilities[j] = partitionAvailabilities[j].subtract(customerPartitionWeight);
                    customerPartitionWeight = new BigDecimal(0);

                }
            }
            if (!list.isEmpty()) {//last element
                customerPartitionProbabilities.put(customersUUIDs.get(i), list);
            }else{
                customerPartitionProbabilities.put(customersUUIDs.get(i), Collections.singletonList(new PartitionInfo(partitionAvailabilities.length-1, 1d)));
            }
        }

        customerPartitionProbabilities.forEach((s, partitionInfos) -> {
            if (partitionInfos.isEmpty())
                System.out.println(s);
        });

        //System.out.println(customerPartitionProbabilities.get("10f47ff7-258b-418e-b2f5-65bddc793120"));
        System.out.println("done");

    }

}
