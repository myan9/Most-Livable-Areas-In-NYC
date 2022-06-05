import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

public class dataCleanReducer extends Reducer<Text, Text, NullWritable, Text> {
  @Override
  public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {  
      for (Text val: value) {
        context.write(NullWritable.get(), val);
      }
  }
}
