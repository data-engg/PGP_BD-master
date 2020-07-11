/*MapReduce program that finds the occurrence of words "hadoop" & "date" and partitions the output into multiple part files 

File Data is :
	Hadoop is an open source framework.
	Data is getting generated day-to-day in TB, PB.
	Hadoop is one of the best tools to handle such huge data.
*/
import java.io.IOException;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class WordPartition extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		int return_status = ToolRunner.run(new Configuration(), new WordPartition(), args);
		System.exit(return_status);

	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost/");
		Job job = Job.getInstance(conf, "Word Partition");
		
		job.setJarByClass(WordPartition.class);
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ShortWritable.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ShortWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setPartitionerClass(WordPartitioner.class);
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		try{
			return (job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
		
		return 0;
	}
	
	static class WordMapper extends Mapper <LongWritable, Text, Text, ShortWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
			String[] values = value.toString().split(" ");
		
			for (String val : values){
				context.write(new Text(val), new ShortWritable((short) 1));
			}
		}
	}

	static class WordReducer extends Reducer <Text , ShortWritable, Text, ShortWritable>{
		@Override
		public void reduce(Text key, Iterable<ShortWritable> values, Context context)
				throws IOException, InterruptedException{
			short count = (short) 0;
			for (ShortWritable val : values){
				count += Short.parseShort(val.toString());
			}			
			context.write(key, new ShortWritable(count));
		}		
	}
	
	static class WordPartitioner extends Partitioner<Text, ShortWritable>{

		@Override
		public int getPartition(Text key, ShortWritable value, int numPartitions) {
			if (key.toString().toLowerCase().equals("hadoop")){
				return 0;
			} else if (key.toString().toLowerCase().equals("data")){
				return 1;
			} else {
				return 2;
			}
		}		
	}	
}
