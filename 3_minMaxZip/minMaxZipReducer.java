import java.io.IOException ;
import java.util.TreeMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.commons.io.output.NullWriter;
import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.mapreduce.Reducer ; 


public class minMaxZipReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
    private TreeMap<String, Integer> total_zip_freq = new TreeMap<String, Integer>();

    @Override
    public void reduce(Text ZIP, Iterable<IntWritable> ZIP_COUNT, Context context) 
        throws IOException, InterruptedException { 

        int totalCount = 0;

        for (IntWritable value : ZIP_COUNT) {
            totalCount += value.get();
        }

        total_zip_freq.put(ZIP.toString(), totalCount);
        context.write(new Text(ZIP), new IntWritable(totalCount));
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        String max_zip = "", min_zip = "";
        int max_val = Integer.MIN_VALUE, min_val = Integer.MAX_VALUE;

        for(Map.Entry<String,Integer> iter : total_zip_freq.entrySet()) {
            String key = iter.getKey();
            Integer value = iter.getValue();
            if(value < min_val) {
                min_zip = key;
                min_val = value;
            }

            if(value > max_val) {
                max_zip = key;
                max_val = value;
            }

        }
        System.out.println("Distinct ZIP #: " + Integer.toString(total_zip_freq.size()));
        System.out.println(max_zip + " : " + Integer.toString(max_val));
        System.out.println(min_zip + " : " + Integer.toString(min_val));
    }
}
