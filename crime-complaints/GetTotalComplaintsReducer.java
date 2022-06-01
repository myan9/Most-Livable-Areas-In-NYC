import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;

public class GetTotalComplaintsReducer extends Reducer<Text, Text, Text, IntWritable> {
  // private TreeMap<String, Integer> statistics = new TreeMap<String, Integer>();
  private ArrayList<Integer> statistics = new ArrayList<Integer>();

  public void reduce(Text zipcode, Iterable<Text> infos, Context context) throws IOException, InterruptedException {
    int total_complaints = 0;

    for (Text _info : infos) {
      total_complaints += 1;
    }

    statistics.add(total_complaints);
    context.write(zipcode, new IntWritable(total_complaints));
  }

  /* get statistics of total complaints */
  public void cleanup(Context context) throws IOException, InterruptedException {
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    int total = 0;
    int count = 0;
    int median = 0;

    Collections.sort(statistics);

    for (Integer complaints : statistics) {
      min = Math.min(min, complaints);
      max = Math.max(max, complaints);
      total += complaints;
      count++;

      if (count == statistics.size() / 2) {
        median = complaints;
      }
    }

    double average = (double) total / statistics.size();

    System.out.printf("%d %d %f %d", min, max, average, median);
  }
}

// profiling:
// 1. how many rows in our cleaned data
// 2. for all the zipcode, what's the min, max, average, median and standard
// deviation of total complaints
// 3. for all the zipcode, how many type of complaints are we dealing with
