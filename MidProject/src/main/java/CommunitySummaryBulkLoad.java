/*
hadoop jar hadoop-CommunitySummaryBulkLoad.jar midproject/hivetables/comPickup_daily_summary midproject/hivetables/comDropoff_daily_summary temp1 temp2 temp3 temp4 communitySummaryEdureka735821
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.fs.FileSystem;

public class CommunitySummaryBulkLoad extends Configured implements Tool {

    public static void main(String args[]) throws Exception{
        int ret_stat = ToolRunner.run(new Configuration(), new CommunitySummaryBulkLoad(), args);
        System.exit(ret_stat);
    }

    public int run(String[] args) throws Exception {

        Path pickUpOutputPath = new Path(args[0]);
        Path dropOffInputPath = new Path(args[1]);
        Path outputPath1 = new Path(args[2]);
        Path outputPath2 = new Path(args[3]);
        Path aggOutput = new Path(args[4]);
        Path outputPath3 = new Path(args[5]);
        TableName tblName = TableName.valueOf(args[6]);

        Configuration conf = new Configuration();
        conf.addResource(HBaseConfiguration.create());
        conf.set("hbase.zookeeper.quorum", "ip-20-0-31-210.ec2.internal");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.mapred.outputtable", args[6]);
        FileSystem fs = FileSystem.get(conf);
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin hadmin = conn.getAdmin();

        if (!hadmin.tableExists(tblName)) {
            HTableDescriptor htable = new HTableDescriptor(tblName);
            htable.addFamily(new HColumnDescriptor("OriginStat"));
            htable.addFamily(new HColumnDescriptor("DestinationStat"));
            htable.addFamily(new HColumnDescriptor("RevenueSummary"));
            hadmin.createTable(htable);
        }

        Table tab = conn.getTable(tblName);
        RegionLocator regLoc = conn.getRegionLocator(tblName);

        // Origin stats load i.e. load values form pick up community
        Job orgStat = Job.getInstance(conf, "Community Pickup Stats");
        orgStat.setJarByClass(CommunitySummaryBulkLoad.class);
        orgStat.setMapperClass(PickUpCommMapper.class);
        orgStat.setMapOutputKeyClass(ImmutableBytesWritable.class);
        orgStat.setMapOutputValueClass(KeyValue.class);

        HFileOutputFormat2.configureIncrementalLoad(orgStat, tab, regLoc);

        FileInputFormat.addInputPath(orgStat, pickUpOutputPath);
        FileOutputFormat.setOutputPath(orgStat, outputPath1);

        if (orgStat.waitForCompletion(true)) {
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(outputPath1, hadmin, tab, regLoc);
            fs.delete(outputPath1, true);
        } else {
            fs.delete(outputPath1, true);
            return 1;
        }
        // Destination stats load i.e. load values form pick up community

        Job desStat = Job.getInstance(conf, "Community DropOff Stats");
        desStat.setJarByClass(CommunitySummaryBulkLoad.class);
        desStat.setMapperClass(DropOffCommMapper.class);
        desStat.setMapOutputKeyClass(ImmutableBytesWritable.class);
        desStat.setMapOutputValueClass(KeyValue.class);

        HFileOutputFormat2.configureIncrementalLoad(desStat, tab, regLoc);

        FileInputFormat.addInputPath(desStat, dropOffInputPath);
        FileOutputFormat.setOutputPath(desStat, outputPath2);
        
        if (desStat.waitForCompletion(true)){
            LoadIncrementalHFiles loader2 = new LoadIncrementalHFiles(conf);
            loader2.doBulkLoad(outputPath2, hadmin, tab, regLoc);
            fs.delete(outputPath2, true);
        } else {
            fs.delete(outputPath2, true);
            return 1;
        }

        Job aggregation = Job.getInstance(conf, "Computation of community wise aggregation");
        aggregation.setJarByClass(CommunitySummaryBulkLoad.class);
        aggregation.setMapperClass(CommRevAggMapper.class);
        aggregation.setReducerClass(CommRevAggReduer.class);
        aggregation.setNumReduceTasks(1);
        aggregation.setMapOutputKeyClass(Text.class);
        aggregation.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(aggregation, pickUpOutputPath, TextInputFormat.class, CommRevAggMapper.class);
        MultipleInputs.addInputPath(aggregation, dropOffInputPath, TextInputFormat.class, CommRevAggMapper.class);
        FileOutputFormat.setOutputPath(aggregation, aggOutput);

        if (!aggregation.waitForCompletion(true)) {
            return 1;
        }

        Job revStat = Job.getInstance(conf, "Community Revenue Stats");
        revStat.setJarByClass(CommunitySummaryBulkLoad.class);
        revStat.setMapperClass(CommRevMapper.class);
        revStat.setMapOutputKeyClass(ImmutableBytesWritable.class);
        revStat.setMapOutputValueClass(KeyValue.class);

        HFileOutputFormat2.configureIncrementalLoad(revStat, tab, regLoc);

        FileInputFormat.addInputPath(revStat, aggOutput);
        FileOutputFormat.setOutputPath(revStat, outputPath3);

        if (revStat.waitForCompletion(true)){
            LoadIncrementalHFiles loader3 = new LoadIncrementalHFiles(conf);
            loader3.doBulkLoad(outputPath3, hadmin, tab, regLoc);
            fs.delete(outputPath3, true);
            return 0;
        } else {
            return 1;
        }

    }
}
