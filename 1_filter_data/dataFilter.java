import org.apache.hadoop.fs.Path ; 
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.mapreduce.Job ; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ; 

public class dataFilter {

    public static void main(String[] args) throws Exception { 
        if (args.length != 2) {
            System.err.println("Usage: dataFilter <input path> <output path>"); 
            System.exit(-1);
        }

        Job job = Job.getInstance(); 
        job.setJarByClass(dataFilter.class); 
        job.setJobName("dataFilter"); 

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 

        job.setMapperClass(dataFilterMapper.class); 
        job.setReducerClass(dataFilterReducer.class);
        // job.setCombinerClass(dataFilterReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion( true ) ? 0 : 1);
    }
}
