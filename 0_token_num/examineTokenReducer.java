import java.io.IOException ;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable ; 
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.mapreduce.Reducer ; 

public class examineTokenReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> { 
    private int total_records = 0;
    private TreeMap<Integer, Integer> stats = new TreeMap<Integer, Integer>();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
        int totalCount = 0;
        for (IntWritable value : values) {
            totalCount += value.get();
        }
        
        total_records += totalCount;
        stats.put(key.get(), totalCount);
        context.write(key, new IntWritable(totalCount));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, Integer> entry : stats.entrySet()) {
            System.out.printf("%d : %.2f%% %d\n", entry.getKey().intValue(), 100.0 * entry.getValue() / total_records, entry.getValue());
        }
    }
}
