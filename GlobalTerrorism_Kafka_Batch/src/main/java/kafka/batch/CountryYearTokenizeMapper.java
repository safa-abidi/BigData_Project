package kafka.batch;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountryYearTokenizeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text countryYear = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("eventid")){
            String[] fields = value.toString().split(",");
            String country = fields[8];
            String year = fields[1];
            countryYear.set(country + "," + year);
            context.write(countryYear, one);
        }
    }

}
