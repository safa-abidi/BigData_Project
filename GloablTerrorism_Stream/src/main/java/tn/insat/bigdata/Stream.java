package tn.insat.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class Stream {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("GlobalTerrorismCount");
        JavaStreamingContext jssc =
                new JavaStreamingContext(conf, Durations.seconds(1));

        String hostname = args[0];
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(hostname, 9999);

        /*JavaRDD<String> rdd = jssc.sparkContext().textFile("/home/ines/Downloads/archive/globalterrorism_2.csv");
        Queue<JavaRDD<String>> queue = new LinkedList<>();
        queue.add(rdd);
        JavaDStream<String> lines = jssc.queueStream(queue);*/

        JavaPairDStream<String, Integer> countryYearPairs = lines
                .mapToPair(line -> {
                    String[] columns = line.split(",");
                    String country = columns[8];
                    String year = columns[1];
                    return new Tuple2<>(country + "-" + year, 1);
                });


        JavaPairDStream<String, Integer> terrorismCount =
                countryYearPairs.reduceByKey((i1, i2) -> i1 + i2);
        terrorismCount.print();
        jssc.start();
        jssc.awaitTermination();
    }
}