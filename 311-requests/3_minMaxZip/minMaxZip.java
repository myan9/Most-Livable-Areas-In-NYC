import org.apache.hadoop.fs.Path ; 
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.mapreduce.Job ; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ; 

public class minMaxZip {
    
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) {
            System.err.println("Usage: minMaxZip <input path> <output path>"); 
            System.exit(-1);
        }

        Job job = Job.getInstance(); 
        job.setJarByClass(minMaxZip.class); 
        job.setJobName("minMaxZip"); 

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 

        job.setMapperClass(minMaxZipMapper.class); 
        job.setReducerClass(minMaxZipReducer.class);
        job.setCombinerClass(minMaxZipReducer.class);

        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1); 
        System.exit(job.waitForCompletion( true ) ? 0 : 1);
    }
}
