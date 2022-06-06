import java.io.IOException ;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable ; 
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.mapreduce.Reducer ; 

public class zip2NameReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
    private ArrayList<Integer> requestFrequencyArray = new ArrayList<Integer>();
    Map<String, Integer> neighborNameZip = new HashMap<String, Integer>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException { 

        int totalCount = 0;
        for (IntWritable value : values) {
            totalCount += value.get();
        }

        requestFrequencyArray.add(totalCount);
        context.write(key, new IntWritable(totalCount));
    }
}
