import java.io.IOException;

import javax.sound.midi.SysexMessage;

import org.apache.hadoop.io.IntWritable ; 
import org.apache.hadoop.io.LongWritable ; 
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.mapreduce.Mapper ; 

public class complaintCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
	
    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException { 

		String line = value.toString(), record = "";
		String[] recordTokenArray = line.split("\\|", -1);
		// unique id, created date, closed date, complaint type, zip, city
		String new_key = recordTokenArray[5] + "|" + recordTokenArray[3] + "|";
		System.out.println(new_key);
		// System.out.println(String.join(" ", recordTokenArray));
		context.write(new Text(new_key), new IntWritable(1));
	}
}
