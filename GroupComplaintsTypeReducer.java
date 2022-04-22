import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.TreeMap;
import java.util.Map;

public class GroupComplaintsTypeReducer extends Reducer<Text, Text, Text, IntWritable> {
    private TreeMap<String, Integer> overral_statistics = new TreeMap<String, Integer>();

    public void reduce(Text zipcode, Iterable<Text> infos, Context context) throws IOException, InterruptedException {
        TreeMap<String, Integer> statistics = new TreeMap<String, Integer>();
        for (Text _info : infos) {
            String info = _info.toString();
            String[] tokens = info.split("\\|");
            String offense_description = tokens[2];

            // statistics for the whole task
            if (overral_statistics.get(offense_description) == null) {
                overral_statistics.put(offense_description, 1);
            } else {
                int count = overral_statistics.get(offense_description);
                overral_statistics.put(offense_description, count + 1);
            }

            // statistics for this zipcode
            if (statistics.get(offense_description) == null) {
                statistics.put(offense_description, 1);
            } else {
                statistics.put(offense_description, statistics.get(offense_description) + 1);
            }
        }

        for (Map.Entry<String, Integer> entry : statistics.entrySet()) {
            String offense = entry.getKey();
            Integer count = entry.getValue();

            // 10003:Bulgary 1000
            context.write(new Text(zipcode.toString() + ":" + offense), new IntWritable(count));
        }
    }

    /* overal statistics */
    public void cleanup(Context context) throws IOException, InterruptedException {
        // 1. how many distinct complaints type exist in the whole dataset
        int distinct_types = overral_statistics.size();
        System.out.printf("distinct type: %d", distinct_types);

        // 2. min max
        int min = Integer.MAX_VALUE;
        String min_type = "";
        int max = Integer.MIN_VALUE;
        String max_type = "";

        for (Map.Entry<String, Integer> entry : overral_statistics.entrySet()) {
            String type = entry.getKey();
            Integer count = entry.getValue();
            if (count < min) {
                min = count;
                min_type = type;
            }

            if (count > max) {
                max = count;
                max_type = type;
            }
        }

        System.out.printf("min type: %s %d", min_type, min);
        System.out.printf("max_type type: %s %d", max_type, max);
    }
}
