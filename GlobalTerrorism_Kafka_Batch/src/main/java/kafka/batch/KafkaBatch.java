package kafka.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaBatch {
    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: <zkQuorum> <group> <topics> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream;
        if (fs.exists(new Path(args[3]))) {
            outputStream = fs.append(new Path(args[3]));
        } else {
            outputStream = fs.create(new Path(args[3]));
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, args[1]);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(args[2]));

        try{
            while (true) {
                ConsumerRecords<String,String> records = consumer.poll(100);
                for (ConsumerRecord<String,String> record : records) {
                    System.out.println(record.key() + " : " + record.value());
                    outputStream.writeBytes(record.value()+"\n");
                }
                outputStream.hsync();
                consumer.commitAsync();
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally {
            fs.close();
        }

    }
}