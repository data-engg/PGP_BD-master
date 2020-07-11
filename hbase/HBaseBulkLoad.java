//This is code loads bulk data from hdfs to hbase. It is a mapreduce code

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HBaseBulkLoad {
	
	public static class HBaseBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>{		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			ImmutableBytesWritable hkey = new ImmutableBytesWritable();
			hkey.set(Bytes.toBytes(key));
			KeyValue kv = new KeyValue(hkey.get(), Bytes.toBytes(<column family>), Bytes.toBytes(<qualifier>), Bytes.toBytes(value.toString()));
			context.write(hkey, kv);			
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3){
			System.out.println("Enter exactly 3 arguments <input hdfs>, <output path>, <table name>");
			System.exit(1);
		}
		String tableName = args[2];

		//Creation of configuration and connection
		Configuration conf = new Configuration();
		conf.addResource(HBaseConfiguration.create());
		conf.set("hbase.zookeeper.quorum", <value>);
		conf.set("hbase.zookeeper.property.clientPort", <value>);
		conf.set("hbase.mapred.outputtable", args[2]);
		System.out.println("Connecting to the server...");
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		System.out.println("Connected to server...");

		//Checking for existance of table{
		if (admin.tableExists(TableName.valueOf(tableName))){
			System.out.println("table exists. Continuing to load...");
		} else {
			System.out.println("Creating table named " + tableName);
			HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(tableName));
			htable.addFamily(new HColumnDescriptor("value"));
			admin.createTable(htable);
			System.out.println("Created table named " + tableName);
		}

		//Configuring job
		Job job = Job.getInstance(conf, "Hbase bulk load");
		job.setJarByClass(HBaseBulkLoad.class);
		job.setMapperClass(HBaseBulkLoadMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		job.setInputFormatClass(TextInputFormat.class);

		//describe table to map reduce
		Table table = conn.getTable(TableName.valueOf(tableName));
		RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(tableName));
		HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

		//setting input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("Hfile created...");

		//loading to hbase table
		System.out.println("Loading data to HBase...");
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
		loader.doBulkLoad(new Path(args[1]), admin, table, regionLocator);
		System.out.println("Driver Terminated");
		System.out.println("Bulk loading completed");
	}
}
