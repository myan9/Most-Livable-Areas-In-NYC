import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;


public class dataProfile1Reducer extends Reducer<Text, Text, NullWritable, Text> {
  @Override
  public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
      int count = 0;  
      for (Text val: value) {
        count += 1;
      }
    context.write(NullWritable.get(), new Text(key + "|" + count));
  }
}
