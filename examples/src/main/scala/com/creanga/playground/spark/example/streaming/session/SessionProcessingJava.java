package com.creanga.playground.spark.example.streaming.session;

import com.fasterxml.jackson.databind.util.ArrayIterator;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;

import java.util.Iterator;

import static org.apache.spark.sql.functions.udf;

public class SessionProcessingJava {


  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("SessionProcessingJava")
        .getOrCreate();

    SQLContext sqlContext = spark.sqlContext();

    UserDefinedFunction driverIdUdf = udf(
        (UDF1<String, String>) SessionProcessingJava::call,
        DataTypes.StringType
    );

//    UDF1 udf = (UDF1<String, String>) SessionProcessingJava::call;
//    spark.udf().register("driverUdf", udf, DataTypes.StringType);


    MemoryStream<String> data = MemoryStream.apply(Encoders.STRING(), sqlContext);
    Dataset<Row> dataSet = data.toDF();
    Dataset<Row> driverDataset = dataSet.withColumn("driverId", driverIdUdf.apply(dataSet.col("value")));

    FlatMapGroupsWithStateFunction<String,Row,Activity,GpsTick> mappingFunction = (key, values, state) -> {
      return new ArrayIterator<>(new GpsTick[]{});
    };

    driverDataset.filter(driverDataset.col("driverId").isNotNull())
        .groupByKey(
            (MapFunction<Row, String>) value -> value.getAs("driverId"), Encoders.STRING()
        )
      .flatMapGroupsWithState(
          mappingFunction,
          OutputMode.Append(),
          Encoders.bean(Activity.class),
          Encoders.bean(GpsTick.class),
          GroupStateTimeout.NoTimeout()
      );
    /**
     val tripDf = data.toDF().
     withColumn("driverId", extractDriverIdUDF(col("value"))).
     filter(col("driverId").isNotNull).
     groupByKey(row => row.getAs[String]("driverId"))
     .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(Mapping.filterNonSessionGpsEvents)

     val query = tripDf.writeStream
     .trigger(Trigger.ProcessingTime("10 seconds"))
     .foreachBatch { (batchDF: Dataset[GpsTick], batchId: Long) =>
     //batchDF.show(20, truncate = false)
     batchDF.coalesce(1).write.
     format("json").
     mode(SaveMode.Overwrite).
     save(s"/home/cornel/stream/$batchId")
     }
     .start


     query.awaitTermination()
     */

    //val data: MemoryStream[String] = MemoryStream[String]
  }

  //driverId: String,
  //      rows: Iterator[Row],
  //      currentState: GroupState[Activity]

//  MapGroupsWithStateFunction<String, Integer, Integer, String> mappingFunction = new MapGroupsWithStateFunction<String, Integer, Integer, String>() {
//        public String call(String key, Iterator<Integer> value, GroupState<Integer> state) {
//        }
//    };


  //    @Override
//    public Iterator<GpsTick> call(Object key, Iterator<Row> values, GroupState<Activity> state) throws Exception {
//      return null;
//    }
  //public interface FlatMapGroupsWithStateFunction<K, V, S, R> extends Serializable {
  //  Iterator<R> call(K key, Iterator<V> values, GroupState<S> state) throws Exception;
  //}

  private static String call(String data) {
    int index1 = data.indexOf("driverId");
    if (index1 == -1)
      return null;
    int index2 = data.indexOf("\"", index1 + 10);
    if (index2 == -1)
      return null;
    int index3 = data.indexOf("\"", index2 + 1);
    if (index3 == -1)
      return null;
    return data.substring(index2 + 1, index3);
  }
}
