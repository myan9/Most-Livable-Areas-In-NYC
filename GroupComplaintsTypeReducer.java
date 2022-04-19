import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.TreeMap;
import java.util.Map;

public class GroupComplaintsTypeReducer extends Reducer<Text, Text, Text, IntWritable> {
    private TreeMap<String, Integer> offense_total_complaints = new TreeMap<String, Integer>();
    public void reduce(Text zipcode, Iterable<Text> infos, Context context) throws IOException, InterruptedException {  
        for (Text _info : infos) {
            String info = _info.toString();
            String[] tokens = info.split("\\|");
            String offense_description = tokens[2]; 

            // record the total complaints by offense
            if (offense_total_complaints.get(offense_description) == null) {
                offense_total_complaints.put(offense_description, 1);
            } else {
                int count = offense_total_complaints.get(offense_description);
                offense_total_complaints.put(offense_description, count + 1);
            }
        }
        
        // write down the total complaints per zipcode
        // write down the total complaints per offense type per zipcode
        for(Map.Entry<String,Integer> entry : offense_total_complaints.entrySet()) {
            String offense = entry.getKey();
            Integer count = entry.getValue();

            context.write(new Text(zipcode.toString() + ":" + offense), new IntWritable(count));
        }

    }
}
