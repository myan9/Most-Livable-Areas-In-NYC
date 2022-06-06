import java.io.IOException ;

import org.apache.commons.io.output.NullWriter;
import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.mapreduce.Reducer ; 

public class dataFilterReducer
    extends Reducer<Text, NullWritable, Text, NullWritable> { 

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException { 
        context.write(new Text(key), NullWritable.get());
    }
}
