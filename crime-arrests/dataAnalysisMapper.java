import java.io.IOException;

import java.util.Arrays;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class dataAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
  final String DELIMITER = "\\|";
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  

    String[] tokens = value.toString().split(DELIMITER);

    String zipcode = tokens[0]; 
    String arrest_date = tokens[1];
    String arrest_desc = tokens[3];
    String borough = tokens[5];

    if (arrest_desc.contains("FORGERY") || arrest_desc.contains("DRUGS") || arrest_desc.contains("FRAUD") || arrest_desc.contains("LAWS")){
      return;
    }
   
    if (borough.length() != 0){
      context.write(new Text(zipcode), new Text(borough));
    }

  }
}


