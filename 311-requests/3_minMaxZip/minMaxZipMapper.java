import java.io.IOException;
import java.util.TreeMap;

import javax.sound.midi.SysexMessage;

import org.apache.hadoop.io.IntWritable ; 
import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.mapreduce.Mapper ; 

public class minMaxZipMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
	
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] recordTokenArray = line.split(";");
        context.write(new Text(recordTokenArray[5].trim()), new IntWritable(1));
	}
}
