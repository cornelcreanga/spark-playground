package com.creanga.playground.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;

import java.util.*;

import static org.apache.spark.sql.types.DataTypes.*;

public class ParquetMap {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .master("local[8]")
                .getOrCreate();

//        StructType schema = new StructType(new StructField[]{
//                new StructField("id", StringType, true, Metadata.empty()),
//                //new StructField("properties", new MapType(IntegerType, DoubleType, false), true, Metadata.empty())}
//                new StructField("properties", new MapType(IntegerType, FloatType, false), true, Metadata.empty())}
//        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", StringType, true, Metadata.empty()),
                //new StructField("properties", new MapType(IntegerType, DoubleType, false), true, Metadata.empty())}
                new StructField("properties",new ArrayType(FloatType,false),true, Metadata.empty())}

        );

        Random r = new Random();
        float rangeMin = 0;
        float rangeMax = 1;

//        List<Row> list = new ArrayList<>();
//        for(int i = 0; i < 1000; i++) {
//            List<Object> row = new ArrayList<>();
//            row.add("AAA" + i);
//            HashMap<Integer, Float> mapValues = new HashMap<>();
//            for (int j = 0; j < 128; j++) {
//                float randomValue = rangeMin + (rangeMax - rangeMin) * r.nextFloat();
//                mapValues.put(j, randomValue);
//            }
//            row.add(JavaConverters.mapAsScalaMapConverter(mapValues).asScala());
//            list.add(RowFactory.create(row.toArray()));
//        }

        List<Row> list = new ArrayList<>();
        for(int i = 0; i < 1000; i++) {
            List<Object> row = new ArrayList<>();
            row.add("AAA" + i);
            float[] values = new float[128];
            for (int j = 0; j < 128; j++) {
                float randomValue = rangeMin + (rangeMax - rangeMin) * r.nextFloat();
                values[j] = randomValue;
            }
            Arrays.sort(values);
            row.add(values);
            list.add(RowFactory.create(row.toArray()));
        }


        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<Row> rdd = jsc.parallelize(list);
        Dataset<Row> ds = sparkSession.createDataFrame(rdd, schema);
        //df.write.option("compression","zstd").mode("overwrite").parquet("/home/cornel/manycolumns/")
        ds.coalesce(1).write().option("compression","zstd").mode("overwrite").parquet("/home/cornel/parquet-float-sorted-array/");
    }


    public static float toFloat( int hbits )
    {
        int mant = hbits & 0x03ff;            // 10 bits mantissa
        int exp =  hbits & 0x7c00;            // 5 bits exponent
        if( exp == 0x7c00 )                   // NaN/Inf
            exp = 0x3fc00;                    // -> NaN/Inf
        else if( exp != 0 )                   // normalized value
        {
            exp += 0x1c000;                   // exp - 15 + 127
            if( mant == 0 && exp > 0x1c400 )  // smooth transition
                return Float.intBitsToFloat( ( hbits & 0x8000 ) << 16
                        | exp << 13 | 0x3ff );
        }
        else if( mant != 0 )                  // && exp==0 -> subnormal
        {
            exp = 0x1c400;                    // make it normal
            do {
                mant <<= 1;                   // mantissa * 2
                exp -= 0x400;                 // decrease exp by 1
            } while( ( mant & 0x400 ) == 0 ); // while not normal
            mant &= 0x3ff;                    // discard subnormal bit
        }                                     // else +/-0 -> +/-0
        return Float.intBitsToFloat(          // combine all parts
                ( hbits & 0x8000 ) << 16          // sign  << ( 31 - 15 )
                        | ( exp | mant ) << 13 );         // value << ( 23 - 10 )
    }

    public static int fromFloat( float fval )
    {
        int fbits = Float.floatToIntBits( fval );
        int sign = fbits >>> 16 & 0x8000;          // sign only
        int val = ( fbits & 0x7fffffff ) + 0x1000; // rounded value

        if( val >= 0x47800000 )               // might be or become NaN/Inf
        {                                     // avoid Inf due to rounding
            if( ( fbits & 0x7fffffff ) >= 0x47800000 )
            {                                 // is or must become NaN/Inf
                if( val < 0x7f800000 )        // was value but too large
                    return sign | 0x7c00;     // make it +/-Inf
                return sign | 0x7c00 |        // remains +/-Inf or NaN
                        ( fbits & 0x007fffff ) >>> 13; // keep NaN (and Inf) bits
            }
            return sign | 0x7bff;             // unrounded not quite Inf
        }
        if( val >= 0x38800000 )               // remains normalized value
            return sign | val - 0x38000000 >>> 13; // exp - 127 + 15
        if( val < 0x33000000 )                // too small for subnormal
            return sign;                      // becomes +/-0
        val = ( fbits & 0x7fffffff ) >>> 23;  // tmp exp for subnormal calc
        return sign | ( ( fbits & 0x7fffff | 0x800000 ) // add subnormal bit
                + ( 0x800000 >>> val - 102 )     // round depending on cut off
                >>> 126 - val );   // div by 2^(1-(exp-127+15)) and >> 13 | exp=0
    }
}
