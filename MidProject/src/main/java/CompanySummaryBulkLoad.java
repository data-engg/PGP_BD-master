import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.RegionLocator;

public class CompanySummaryBulkLoad extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int ret_stat = ToolRunner.run(new Configuration(), new CompanySummaryBulkLoad(), args);
        System.exit(ret_stat);
    }
    public int run(String[] args) throws IOException{
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        TableName tblName = TableName.valueOf(args[2]);

        Configuration conf = new Configuration();
        conf.addResource(HBaseConfiguration.create());
        conf.set("hbase.zookeeper.quorum", "ip-20-0-31-210.ec2.internal");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.mapred.outputtable", args[2]);

        Connection conn = ConnectionFactory.createConnection(conf);
        Admin hadmin = conn.getAdmin();

        if (! hadmin.tableExists(tblName)){
            HTableDescriptor htable = new HTableDescriptor(tblName);
            htable.addFamily(new HColumnDescriptor("TripCountStats"));
            htable.addFamily(new HColumnDescriptor("RevenueDetails"));
            hadmin.createTable(htable);
        }

        Job job = Job.getInstance(conf, "Company Summary bulk loading");
        job.setJarByClass(CompanySummaryBulkLoad.class);
        job.setMapperClass(CompanySummaryBulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        Table tab = conn.getTable(tblName);
        RegionLocator regLoc = conn.getRegionLocator(tblName);
        HFileOutputFormat2.configureIncrementalLoad(job, tab, regLoc);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        try{
            job.waitForCompletion(true);
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(outputPath, hadmin, tab, regLoc);
            return 0;
        } catch (ClassNotFoundException e){
            e.printStackTrace();
            return 1;
        } catch (InterruptedException e){
            e.printStackTrace();
            return 1;
        } catch (Exception e){
            e.printStackTrace();
            return 1;
        }
    }


    static class CompanySummaryBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue >{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().trim().split(",");
            ImmutableBytesWritable rowid = new ImmutableBytesWritable();
            KeyValue kv;
            rowid.set(Bytes.toBytes(token[0]));

            kv = new KeyValue(rowid.get(), Bytes.toBytes("TripCountStats"),
                    Bytes.toBytes("daily_trip_count".concat(" " + token[1])),
                    Bytes.toBytes(token[2]));
            context.write(rowid, kv);
            kv = new KeyValue(rowid.get(), Bytes.toBytes("TripCountStats"),
                    Bytes.toBytes("daily_total_fare".concat(" " + token[1])),
                    Bytes.toBytes(token[3]));
            context.write(rowid, kv);
            kv = new KeyValue(rowid.get(), Bytes.toBytes("TripCountStats"),
                    Bytes.toBytes("daily_total_distance".concat(" " + token[1])),
                    Bytes.toBytes(token[4]));
            context.write(rowid, kv);
            kv = new KeyValue(rowid.get(), Bytes.toBytes("TripCountStats"),
                    Bytes.toBytes("daily_total_duration".concat(" " + token[1])),
                    Bytes.toBytes(token[5]));
            context.write(rowid, kv);
            kv = new KeyValue(rowid.get(), Bytes.toBytes("RevenueDetails"),
                    Bytes.toBytes("daily_average_amount".concat(" " + token[1])),
                    Bytes.toBytes(token[6]));
            context.write(rowid, kv);
            kv = new KeyValue(rowid.get(), Bytes.toBytes("RevenueDetails"),
                    Bytes.toBytes("daily_average_distance".concat(" " + token[1])),
                    Bytes.toBytes(token[7]));
            context.write(rowid, kv);
            kv = new KeyValue(rowid.get(), Bytes.toBytes("RevenueDetails"),
                    Bytes.toBytes("daily_average_duration".concat(" " + token[1])),
                    Bytes.toBytes(token[8]));
            context.write(rowid, kv);
        }
    }
}
