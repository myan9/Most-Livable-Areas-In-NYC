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
		// unique id, created date, closed date, complaint type, zip, city
		String[] recordTokenArray = line.split("|");

		
		context.write(new Text(recordTokenArray[4].trim()), new IntWritable(1));

	}
}
