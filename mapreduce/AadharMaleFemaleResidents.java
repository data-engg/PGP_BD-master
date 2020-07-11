/* For that attached dataset aadhar_data.csv, this job performs three tasks. 3 mapreduce jobs will run in this job, they are: 
*	1. Count the total male and female population state-wise ---> job
*	2. Count the number of rejected application state-wise ---> job2
*	3. Count the average rejections in the country ---> job3
* Output file created by job2 is fed as input into job3
*/
import java.io.IOException;
import  org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.fs.Path;


public class AadharMaleFemaleResidents extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int return_status = ToolRunner.run(new Configuration(), new AadharMaleFemaleResidents(), args);
        System.exit(return_status);
    }
    
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf =new Configuration();
        Job job = Job.getInstance(conf, "Aadhar Male and Female count state-wise"); 
        Job job2 = Job.getInstance(conf, "Rejected application count state-wise");
        Job job3 = Job.getInstance(conf, "Average rejections in country");
        
        job.setJarByClass(AadharMaleFemaleResidents.class);
        job.setMapperClass(AadharMaleFemaleResidentsMapper.class);
        job.setReducerClass(AadharMaleFemaleResidentsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job2.setJarByClass(AadharMaleFemaleResidents.class);
        job2.setMapperClass(RejectedResidentsMapper.class);
        job2.setCombinerClass(RejectedResidentsReduer.class);
        job2.setReducerClass(RejectedResidentsReduer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
        job3.setJarByClass(AadharMaleFemaleResidents.class);
        job3.setMapperClass(RejectAvgMapper.class);
        job3.setReducerClass(RejectAvgReduer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(FloatWritable.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        
        try {
            int job_status = (job.waitForCompletion(true) ? 0 : 1);
            int job2_status = (job2.waitForCompletion(true) ? 0 : 1);
            int job3_status = (job3.waitForCompletion(true) ? 0 : 1);
            if (job_status == 1 && job2_status == 1 && job3_status == 1)
                return 1;
            else
                return 0;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();            
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        
        return 0;
    }
    

    public static class AadharMaleFemaleResidentsMapper extends Mapper<LongWritable, Text, Text, Text>{
	    //This map task selects state and gender columns from the dataset. {key : state, values : gender}
	    //This is the mapper for the job "job"
        @Override
        public void map(LongWritable key, Text value, Context context) 
            throws InterruptedException, IOException{
            String[] values=value.toString().split(",");
            String state = values[2];
            String gender = values[6];
            context.write(new Text(state), new Text(gender));
        }
    }
    
    public static class AadharMaleFemaleResidentsReducer extends Reducer<Text, Text, Text, Text>{
	    //This reduce task aggregates the count of males and females state-wise {key : state, value : "count(male) + count(female)"}
	    //This is the reducer for job "job"
        @Override
        public void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException{
            int males=0;
            int females=0;
            for (Text val : value){
                if (val.toString().equals("M"))
                    males ++;
                else if (val.toString().equals("F"))
                    females++;
            }
            String result = "male = " + males + " : females = " + females;
            context.write(key, new Text(result));
        }
    }
    
    public static class RejectedResidentsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	    //This map task selects state and reject_status columns from the dataset. {key : state, values : rejection_status}
	    //This is the mapper for the job "job2"
        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            String[] values = value.toString().split(",");
            String state = values[2];
            int reject_status = Integer.parseInt(values[9]);
            if (reject_status == (short) 1){
                 context.write(new Text(state), new IntWritable(reject_status));
            }
        }
    }
    
    public static class RejectedResidentsReduer extends Reducer<Text, IntWritable, Text , IntWritable>{
	    //This reduce task aggregates the count of rejection status state-wise {key : state, value : count(reject_status)}
	    //This is the reducer for job "job2"
        @Override
        public void reduce(Text key, Iterable<IntWritable> value, Context context)
            throws IOException, InterruptedException{
            int count = 0;
            for (IntWritable val:value)
                count+=val.get();
            context.write(key, new IntWritable(count));
        }
    }
    
    public static class RejectAvgMapper extends Mapper<Text, Text, Text , IntWritable>{
	    /*This map task flattens the reject_status across all states to a single records. E.g
	    	Input : (A,12),(B,15),(c,20)
		Output : ("1", [12,15,20])
	     This the mapper for job "job3"
	    */
        @Override
        public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException{
            int rejection = Integer.parseInt(value.toString().trim());
            context.write(new Text("1"), new IntWritable(rejection));
        }
    }
    
    public static class RejectAvgReduer extends Reducer<Text, IntWritable, Text, FloatWritable>{
	@Override
	    //This reduce task aggregates the averages rejection count across states
	    //This is the reducer for job "job3"
	public void reduce(Text key, Iterable<IntWritable> value, Context context)
		throws IOException, InterruptedException{
			int no_of_obs=0, sum_of_obs=0;
			for (IntWritable val:value){
			    sum_of_obs += val.get();
			    no_of_obs++;
			}
			context.write(new Text("Average rejections in country is : "), new FloatWritable(sum_of_obs/no_of_obs));
		}
	}
}
