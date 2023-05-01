package kafka.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class KafkaStream {
    private static final Pattern SPACE = Pattern.compile(" ");

    private KafkaStream() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: SparkKafka <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("SparkKafka");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                new Duration(2000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        JavaDStream<String> lines = messages.map(Tuple2::_2).filter(line -> !line.trim().isEmpty());
        ;

        JavaPairDStream<String, Integer> countryYearPairs = lines
                .mapToPair(line -> {
                    String[] columns = line.split(",");
                    String country = "";
                    String year = "";
                    if (columns.length >= 9) {
                        country = columns[8];
                    }
                    if (columns.length >= 2) {
                        year = columns[1];
                    }
                    return new Tuple2<>(country + "-" + year, 1);
                });


        JavaPairDStream<String, Integer> terrorismCount =
                countryYearPairs.reduceByKey((i1, i2) -> i1 + i2);

        JavaPairDStream<String, Integer> countryAttackCount = terrorismCount
                .mapToPair( pair -> {
                    String[] parts = pair._1().split("-");
                    String country = parts[0];
                    int count = pair._2();
                    return new Tuple2<>(country,count);
                }).reduceByKey((c1,c2)-> c1+c2).mapToPair(Tuple2::swap).transformToPair(pair-> pair.sortByKey(false)).mapToPair(Tuple2::swap);
        countryAttackCount
                .map(pair -> pair._1() + "," + pair._2())
                .print();
        jssc.start();
        jssc.awaitTermination();
    }
}
