import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

public class MapredSkeleton extends Configured implements Tool{
    public static void main(String[] args){
        try{
            int returnStatus = ToolRunner.run(new Configuration(), new MapredSkeletoon(), args);
            System.exit(returnStatus);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public int run (String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "Job name");
        
        job.setJarByClass(MapredSkeletoon.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        try {
            return job.waitForCompletion(true) ? 0 : 1;
        } catch(ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        
        return 0;
    }
    
    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            //Map side Business logic
        }
    }
    
    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{
            //Redue side Business logic
        }        
    }
}
