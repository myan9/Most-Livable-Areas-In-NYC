import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.TreeMap;
import java.util.Map;

public class GetTotalComplaintsReducer extends Reducer<Text, Text, Text, IntWritable> {
  // private TreeMap<String, Integer> year_total_complaints = new TreeMap<String, Integer>();
  // private TreeMap<String, Integer> offense_total_complaints = new TreeMap<String, Integer>();
  
  public void reduce(Text zipcode, Iterable<Text> infos, Context context) throws IOException, InterruptedException {  
    int total_complaints = 0;
    
    for (Text _info : infos) {
      total_complaints += 1;
      
      String info = _info.toString();
      String[] tokens = info.split("\\|");
      String report_date = tokens[0];
      String offense_code = tokens[1];
      String offense_description = tokens[2]; 

      //record the total complaints by year
      String[] dates = report_date.split("/");
      String year = dates[2];
      
      if (year_total_complaints.get(year) == null) {
        year_total_complaints.put(year, 1);
      } else {
        int count = year_total_complaints.get(year);
        year_total_complaints.put(year, count + 1);
      }

      // record the total complaints by offense
       if (offense_total_complaints.get(offense_description) == null) {
        offense_total_complaints.put(offense_description, 1);
      } else {
        int count = offense_total_complaints.get(offense_description);
        offense_total_complaints.put(offense_description, count + 1);
      }
    }
    
    // write down the total complaints per zipcode
    context.write(zipcode, new IntWritable(total_complaints));
    // write down the total complaints per year per zipcode
    for(Map.Entry<String,Integer> entry : year_total_complaints.entrySet()) {
      String year = entry.getKey();
      Integer count = entry.getValue();

      context.write(new Text(zipcode.toString() + ":" + year), new IntWritable(count));
    }
    // write down the total complaints per offense type per zipcode
    for(Map.Entry<String,Integer> entry : offense_total_complaints.entrySet()) {
      String offense = entry.getKey();
      Integer count = entry.getValue();

      context.write(new Text(zipcode.toString() + ":" + offense), new IntWritable(count));
    }

  }
}
