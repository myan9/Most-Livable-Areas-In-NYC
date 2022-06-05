import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;


public class dataStatsReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> counts = new ArrayList<Integer>(); 
        int max = 0 ;
        int min = 0 ; 
        int mean = 0 ;
        int median = 0; 
        int total = 0; 
        int c = 0; 
        for (Text val: value) {
            c = Integer.parseInt(val.toString());
            counts.add(c);
            total += c; 
        }
        Collections.sort(counts);
        min = counts.get(0);
        max = counts.get(counts.size()-1);
        median = counts.get(counts.size()/2);
        mean = total / counts.size(); 
        context.write(NullWritable.get(), new Text("min" + "=" + min));
        context.write(NullWritable.get(), new Text("max" + "=" + max));
        context.write(NullWritable.get(), new Text("median" + "=" + median));
        context.write(NullWritable.get(), new Text("mean" + "=" + mean));

    }
}
