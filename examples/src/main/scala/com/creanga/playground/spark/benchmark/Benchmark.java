package com.creanga.playground.spark.benchmark;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.math.BigDecimal;
import java.math.MathContext;

public class Benchmark {


    public static void main(String[] args) {
        int n = 100000000;
        BigDecimal sum = new BigDecimal(0);
        double pi25=3.141592653589793238462643;
        //          3.141592653589794071795976716612836
        //          3.141592653589793246795976716612836

        long t1 = System.currentTimeMillis();
        for (int i = 1; i <= n; i++) {
            BigDecimal bigDecimal = new BigDecimal(i-0.5);
            bigDecimal = bigDecimal.divide(new BigDecimal(n), MathContext.DECIMAL128);
            bigDecimal = bigDecimal.multiply(bigDecimal);
            //double d = ((double)i-0.5)/n;
            bigDecimal = new BigDecimal(1).add(bigDecimal);
            bigDecimal = new BigDecimal(4).divide(bigDecimal, MathContext.DECIMAL128);
            sum = sum.add(bigDecimal);
            //sum += 4 / (1 + d*d);
        }
        long t2 = System.currentTimeMillis();
        BigDecimal pib = sum.divide(new BigDecimal(n), MathContext.DECIMAL128);
        System.out.println(pib.toPlainString());
//        double pi = sum/n;
//        System.out.println(pi);
//        System.out.println(Math.abs(pi-pi25));
//        System.out.println(t2-t1);

    }

}
