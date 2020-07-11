/*
	This is a map reduce code to calculate inverted index on first name 
*/

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

public class InvertedIndex extends Configured implements Tool{
    public static void main(String[] args){
        try{
            int returnStatus = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
            System.exit(returnStatus);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public int run (String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		
		if ( args.length != 2 ){
			System.out.println("Enter exactly 2 arguements");
			System.out.println("E.g. hadoop jar InvertedIndex.jar <input path> <output path>");
			return 1;
		}
		
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "Job name");
        
        job.setJarByClass(MapredSkeletoon.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
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
			return 1;
        } catch (InterruptedException e){
            e.printStackTrace();
			return 1;
        }
    }
    
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			String[] records = value.toString().split(",");
			String fname = records[0];
			if( ! fname.equals("First_Name") ){
				context.write( new Text(fname), new Text(filename));
			}
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			String files = "";
			for ( Text value : values ){
				files += (value.toString() + "\t");
			}
			context.write(key, new Text(files));
		}
	}
}
