import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.TreeMap;
import java.util.Map;

public class GetTotalComplaintsReducer extends Reducer<Text, Text, Text, IntWritable> {
  public void reduce(Text zipcode, Iterable<Text> infos, Context context) throws IOException, InterruptedException {  
    int total_complaints = 0;
    
    for (Text _info : infos) {
      total_complaints += 1;
    }
    
    // write down the total complaints per zipcode and print them in sorted order
    // 10003  654 Felony 28 Gun 20 Bla 300
    // 10004  600
    context.write(zipcode, new IntWritable(total_complaints));
  }
}
