package kafka.stream;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.Document;
import scala.Tuple2;


import java.util.*;
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

        JavaPairDStream<String, Integer> countryYearPairs = lines
                .mapToPair(line -> {
                    if(line.isEmpty() || line.trim().isEmpty()) return null;
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
                }).reduceByKey((c1,c2)-> c1+c2).mapToPair(Tuple2::swap).transformToPair(pair-> pair.sortByKey(false)).mapToPair(Tuple2::swap).repartition(1);;


        String uri = "mongodb+srv://admin:admin@cluster0.j2nushr.mongodb.net/?retryWrites=true&w=majority";

        countryAttackCount.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                try {
                    List<Document> pairs = new ArrayList<>();
                    partition.forEachRemaining(pair -> {
                        String country = pair._1();
                        Integer count = pair._2();
                        if(country != ""){
                            Document doc = new Document("country", country)
                                    .append("count", count);
                            pairs.add(doc);
                        }

                    });

                    if(!pairs.isEmpty()){
                        ConnectionString mongoURI = new ConnectionString(uri);
                        MongoClientSettings settings = MongoClientSettings.builder()
                                .applyConnectionString(mongoURI)
                                .build();
                        MongoClient mongoClient = MongoClients.create(settings);
                        MongoDatabase database = mongoClient.getDatabase("GlobalTerrorism");
                        MongoCollection<Document> collection = database.getCollection("stream");
                        Document doc = new Document("pairs", pairs);
                        System.out.println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO "+pairs.size());
                        collection.insertOne(doc);
                        mongoClient.close();
                    }

                }
                catch (Exception e){

                }
            });
        });

        /*countryAttackCount.foreachRDD(rdd -> {
            rdd.foreach(pair -> {
                String country = pair._1();
                Integer count = pair._2();
                if(country != ""){
                    Document doc = new Document("country", country)
                            .append("count", count);

                    ConnectionString mongoURI = new ConnectionString(uri);
                    MongoClientSettings settings = MongoClientSettings.builder()
                            .applyConnectionString(mongoURI)
                            .build();
                    MongoClient mongoClient = MongoClients.create(settings);
                    MongoDatabase database = mongoClient.getDatabase("GlobalTerrorism");
                    MongoCollection<Document> collection = database.getCollection("stream");
                    System.out.println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO "+doc.toJson());
                    collection.insertOne(doc);
                    mongoClient.close();
                }
            });
        });*/


        jssc.start();
        jssc.awaitTermination();
    }
}
