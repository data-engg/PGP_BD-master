import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;


public class ReduceSideJoinCustTxns extends Configured implements Tool{
    
    public static void main(String[] args) throws Exception{
        int return_status = ToolRunner.run(new Configuration(), new ReduceSideJoinCustTxns(), args);
        System.exit(return_status);        
    }

    public int run(String[] args) throws IOException{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total transtion by cust id");
        job.setJarByClass(ReduceSideJoinCustTxns.class);
        job.setMapperClass(TransRollupMapper.class);
        job.setReducerClass(TransRollupReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        Job job2 = Job.getInstance(conf, "total transaction by customer");
        job2.setJarByClass(ReduceSideJoinCustTxns.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(ReducerJoin.class);
        job2.setNumReduceTasks(1);
        
        
        MultipleInputs.addInputPath(job2, new Path(args[1]), KeyValueTextInputFormat.class, TransMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, CustMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));       
        
        try{
            int job_ret_stat =  (job.waitForCompletion(true)?0:1);
            int job2_ret_stat = (job2.waitForCompletion(true)?0:1);
            if (job_ret_stat == 0 && job2_ret_stat == 0)
                return 0;
            else 
                return 1;
            
        } catch (ClassNotFoundException e){
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        return 0;        
    }
    
    public static class TransRollupMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable>{
        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            String[] values = value.toString().split(",");
            if (values.length == 9){
                int cust_id = Integer.parseInt(values[2]);
                float trans_amt = Float.parseFloat(values[3]);
                context.write(new IntWritable(cust_id), new FloatWritable(trans_amt));
            }
        }
    }
    
    public static class TransRollupReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>{
        @Override
        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException{
            float total=0;
            for (FloatWritable val : values){
                total += val.get();
            }
            context.write(key, new FloatWritable(total));
        }
    }
    
    public static class TransMapper extends Mapper<Text, Text, IntWritable, Text>{
        @Override
        public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException{
                context.write(new IntWritable(Integer.parseInt(key.toString().trim())), new Text("trans\t"+value.toString()));
            }
        }        
    
    public static class CustMapper extends Mapper<LongWritable, Text, IntWritable, Text >{
        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            String[] values = value.toString().split(",");
            if (values.length == 5){
                int cust_id = Integer.parseInt(values[0]);
                String cust_name = values[1] + " " + values[2];
                context.write(new IntWritable(cust_id), new Text("cust\t"+cust_name));
            }
        }
    }
    
    public static class ReducerJoin extends Reducer<IntWritable, Text, Text, Text>{
        @Override
        public void reduce(IntWritable key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException{
            ArrayList<String> trans = new ArrayList<String>();
            ArrayList<String> custs = new ArrayList<String>();
            
            for (Text val : value){
                String[] parts = val.toString().split("\t");
                if (parts[0].equals("trans")){
                    trans.add(parts[1]);
                } else if (parts[0].equals("cust")){
                    custs.add(parts[1]);
                }
            }
            
            for (String cust : custs){
                for (String tran : trans){
                    context.write(new Text(cust), new Text(tran));
                }
            }
            
        }
    }
}

