package tn.insat.bigdata;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class AttackCounter {
    public static void main(String[] args) throws Exception {




        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "attack counter");
        job.setJarByClass(AttackCounter.class);
        job.setMapperClass(CountryYearTokenizeMapper.class);
        job.setReducerClass(AttackCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if(job.waitForCompletion(true)){
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(args[1]));
            for (int i = 0; i < status.length; i++) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                List<Document> docs = new ArrayList<>();
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\t");
                    String[] countryYear = fields[0].split(",");

                    Document doc = new Document("country", countryYear[0])
                            .append("year", Integer.parseInt(countryYear[1]))
                            .append("attacks", Integer.parseInt(fields[1]));
                    docs.add(doc);
                    //collection.insertOne(doc);
                }
                if(!docs.isEmpty()){
                    String uri = "mongodb+srv://admin:admin@cluster0.j2nushr.mongodb.net/?retryWrites=true&w=majority";
                    ConnectionString mongoURI = new ConnectionString(uri);
                    MongoClientSettings settings = MongoClientSettings.builder()
                            .applyConnectionString(mongoURI)
                            .build();
                    MongoClient mongoClient = MongoClients.create(settings);
                    MongoDatabase database = mongoClient.getDatabase("GlobalTerrorism");
                    MongoCollection<Document> collection = database.getCollection("batch");

                    Document doc = new Document("pairs", docs);
                    collection.insertOne(doc);
                    mongoClient.close();
                }
            }

            System.exit(0);
        } else {
            System.exit(1);
        }

        System.exit(1);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}