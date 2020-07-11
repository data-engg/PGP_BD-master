//For the dataset uploaded in "prices.csv.zip" this mapreduce code calcualtes the lowest price of a stock

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

public class StockMinPrice extends Configured implements Tool{
	
    public static void main(String[] args){
        try{
            int returnStatus = ToolRunner.run(new Configuration(), new StockMinPrice(), args);
            System.exit(returnStatus);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }    
    
    public static class StockMinPriceMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
        @Override
        public void map(LongWritable key, Text value, Context context) 
        		throws IOException, InterruptedException{
            String[] values = value.toString().split(",");
			if (! values[0].toLowerCase().equals("date"))
				context.write(new Text(values[1]), new FloatWritable(Float.parseFloat(values[5])));
        }
    }
    
    public static class StockMinPriceReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
        		throws IOException, InterruptedException{
            float min_price = Float.MAX_VALUE;
			for ( FloatWritable val : values)
				min_price = (val.get() < min_price) ? val.get() : min_price;  
			context.write(key, new FloatWritable(min_price));
		}
    }
	
	public int run (String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "Lowest price of a stock");
        
        job.setJarByClass(StockMinPrice.class);
        job.setMapperClass(StockMinPriceMapper.class);
        job.setReducerClass(StockMinPriceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
		job.setNumReduceTasks(1);  
        
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
}
