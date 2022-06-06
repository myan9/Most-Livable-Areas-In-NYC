import java.io.IOException ;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable ; 
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.mapreduce.Reducer ; 

public class complaintCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
    private ArrayList<Integer> requestFrequencyArray = new ArrayList<Integer>();

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

    // public void cleanup(Context context) throws IOException, InterruptedException {
    //     Collections.sort(requestFrequencyArray);
    //     int length = requestFrequencyArray.size();

    //     int freq_min = requestFrequencyArray.get(0), freq_max = requestFrequencyArray.get(length-1);

    //     double freq_median = 0.0, freq_mean = 0.0;

    //     if(length % 2 == 0) {
    //         freq_median = (requestFrequencyArray.get(length / 2) + requestFrequencyArray.get(length / 2 + 1)) / 2.0;
    //     } else {
    //         freq_median = requestFrequencyArray.get(length / 2);
    //     }

    //     int total_sum = requestFrequencyArray.stream().reduce(Integer::sum).get().intValue();
    //     freq_mean = (double) total_sum / length;
        
    //     System.out.println("freq_min: " + Integer.toString(freq_min));
    //     System.out.println("freq_max: " + Integer.toString(freq_max));
    //     System.out.println("freq_median: " + Double.toString(freq_median));
    //     System.out.println("freq_mean: " + Double.toString(freq_mean));
    //     System.out.println("Distinct # of requests: " + Integer.toString(length));
    // }
}
