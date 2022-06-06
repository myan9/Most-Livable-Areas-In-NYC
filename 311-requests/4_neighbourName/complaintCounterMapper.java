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
		Map<Integer, String> neighborNameZip = Map.ofEntries(
			"Upper West Side": [10023, 10024, 10025, 10069],
			"Midtown West": [10019, 10018, 10036, 10001],
			"Chelsea": [10001, 10011],
			"West Village": [10014],
			"SOHO": [10013, 10012],
			"Tribeca": [10007, 10013, 10282],
			"Battery Park": [10280, 10004, 10282, 10006],
			"Financial District": [10004, 10005, 10006, 10038],
			"Upper East Side": [10021, 10065, 10075, 10128, 10028, 10022],
			"Midtown East": [10022, 10017],
			"Murray Hill": [10016],
			"Gramercy Flatiron": [10010],
			"East Village": [10003, 10009, 10002]
		);


		String line = value.toString(), record = "";
		String[] recordTokenArray = line.split("\\|", -1);
		
		// zip, complaint, count

		String new_key = recordTokenArray[5] + "|" + recordTokenArray[3] + "|";
		System.out.println(new_key);
		// System.out.println(String.join(" ", recordTokenArray));
		context.write(new Text(new_key), new IntWritable(1));
	}
}
