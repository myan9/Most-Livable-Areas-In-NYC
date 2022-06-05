import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class dataProfile1Mapper extends Mapper<LongWritable, Text, Text, Text> {
  final String DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  

    String[] tokens = value.toString().split(DELIMITER, -1);

    String arrest_description = tokens[5]; 
   
    context.write(new Text(arrest_description), new Text("1"));
  }
}