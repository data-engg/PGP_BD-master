import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.io.IOException;


public class CommRevMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ImmutableBytesWritable rowid = new ImmutableBytesWritable();
        KeyValue kv;
        String tokens[] = value.toString().trim().split(",");
        if (tokens.length == 3){

            rowid.set(Bytes.toBytes(tokens[0]));
            kv = new KeyValue(rowid.get(),
                    Bytes.toBytes("RevenueSummary"),
                    Bytes.toBytes("daily total revenue"),
                    Bytes.toBytes(tokens[1]));
            context.write(rowid, kv);

            kv = new KeyValue(rowid.get(),
                    Bytes.toBytes("RevenueSummary"),
                    Bytes.toBytes("daily average revenue"),
                    Bytes.toBytes(tokens[2]));

            context.write(rowid, kv);
        }
    }
}
