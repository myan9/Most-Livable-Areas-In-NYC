import java.io.IOException;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sound.midi.SysexMessage;

import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;

public class examineTokenMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> { 
    private HashMap<Integer, String> token_example_map = new HashMap<Integer, String>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
		String line = value.toString();
		// String[] recordTokenArray = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		String[] recordTokenArray = line.split(",", -1);
        boolean is_empty_token = false, print_once = false;
        for(String token : recordTokenArray) {
            if(!is_empty_token && token.isEmpty()) {
                System.out.print("We have at least one empty token\n");
                is_empty_token = true;
            }
        }
        
        // if(!token_example_map.containsKey(recordTokenArray.length)) {
        //     String result = Stream.of(recordTokenArray)
        //                         .filter(s -> s != null & !s.isEmpty())
        //                         .collect(Collectors.joining("|"));
        //     // token_example_map.put(recordTokenArray.length, String.join("|", recordTokenArray));
        //     token_example_map.put(recordTokenArray.length, result);
        // }

        if(!print_once) {
            System.out.println(String.join("|", recordTokenArray));
            print_once = true;
        }

        context.write(new IntWritable(recordTokenArray.length), new IntWritable(1));
	}

    // @Override
    // protected void cleanup(Context context) throws IOException, InterruptedException {
    //     for (Map.Entry<Integer, String> entry : token_example_map.entrySet()) {
    //         System.out.println(Integer.toString(entry.getKey()) + " : " + entry.getValue());
    //     }
    // }
}
