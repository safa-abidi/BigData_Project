package kafka.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaBatch {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream = fs.create(new Path(args[3]));

        // Define Kafka consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, args[1]);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to Kafka topics
        consumer.subscribe(Collections.singletonList(args[2]));

        // Poll for records
        try{
            while (true) {
                //Map<String, ConsumerRecords<String,String>> records = consumer.poll(100);

                ConsumerRecords<String,String> records = consumer.poll(100);
                // populate the map
            /*for (Map.Entry<String, ConsumerRecords<String,String>> entry : map.entrySet()) {
                String key = entry.getKey();
                ConsumerRecords<String,String> value = entry.getValue();
                // do something with the key-value pair
                System.out.println(key + " : " + value);
            }*/
                for (ConsumerRecord<String,String> record : records) {
                    // Process records
                    System.out.println(record.key() + " : " + record.value());
                    outputStream.writeBytes(record.value()+"\n");
                }
                outputStream.hsync();
                // Commit offsets
                consumer.commitAsync();
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally {
            fs.close();
        }

        /*Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "attack counter");
        job.setJarByClass(KafkaBatch.class);
        job.setMapperClass(CountryYearTokenizeMapper.class);
        job.setReducerClass(AttackCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
    }
}