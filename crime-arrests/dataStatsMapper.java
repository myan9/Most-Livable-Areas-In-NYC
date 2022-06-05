import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class dataStatsMapper extends Mapper<LongWritable, Text, Text, Text> {
  final String DELIMITER = "\\|";

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  

    String[] tokens = value.toString().split(DELIMITER);
    System.out.println(tokens[1]);
   
    context.write(new Text("1"), new Text(tokens[1]));
  }
}