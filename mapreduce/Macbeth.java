import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueInputFormat;
import org.apache.hadoop.fs.Path;

public class Macbeth extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		 int returnStatus = ToolRunner.run(new Configuration(), new Macbeth(), args);
         System.exit(returnStatus);
	}
	
	public int run(String[] args) throws Exception{
		
		/*
		if (args.length != 2){
			System.out.println("Enter two arguments");
			System.out.println("E.g.  hadoop jar Macbeth.jar <input_file> <output_file>");
			return 1;
		}*/
		
		Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "Macbeth word count");
		Job job2 = Job.getInstance(conf, "Macbeth word count");
        job.setJarByClass(Macbeth.class);
        job.setMapperClass(MacbethMapper.class);
        job.setReducerClass(MacbethReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job2.setJarByClass(Macbeth.class);
        job2.setMapperClass(MacbethFilterMapper.class);       
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
		job2.setInpuFormatClass(KeyValueInputFormat.class)
        job2.setNumReduceTasks(0);
		
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
        if (job.waitForCompletion(true)){
			if (job2.waitForCompletion(true)){
				return 0;
			} else {
			return 1;
		}
		return 1;
		}
        
        
	}
	
	public static class MacbethFilterMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
		
		public void map(Text key, IntWritable value, Context context)
		throws InterruptedException, IOException{
			if (value.get < 100){
				context.write(key, value);
			}
		}
	}
	
	public static class MacbethMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context context)
		throws InterruptedException, IOException{
			String[] words = value.toString().split("\\s");
			for (String word : words){
				String word_filtered = word.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
				if (word_filtered.length() > 5){
					context.write(new Text(word_filtered), new IntWritable(1));
				}
			}
		}
	}
	
	public static class MacbethReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException{
			Integer sum = 0;
			for (IntWritable val : values){
				sum += val.get();
			}
			if (sum > 100)
			context.write(key, new IntWritable(sum));
		}
	}

}
