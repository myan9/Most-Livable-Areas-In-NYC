import java.io.IOException;

import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.lang.Math;

public class ZipcodeMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
    String line = value.toString();
    
    String[] tokens = line.split("\\|");
    String zipcode = tokens[0];
    String others = line.substring(6);

    context.write(new Text(zipcode), new Text(others));
  }
}