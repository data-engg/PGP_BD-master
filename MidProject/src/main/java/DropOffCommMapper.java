import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DropOffCommMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>{
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().trim().split(",");
        ImmutableBytesWritable rowid = new ImmutableBytesWritable();
        KeyValue kv;

        if (tokens.length == 7){
            rowid.set(Bytes.toBytes(tokens[0]));
            kv = new KeyValue(rowid.get(),
                    Bytes.toBytes("DestinationStat"),
                    Bytes.toBytes("daily_trip_count"),
                    Bytes.toBytes(tokens[1]));
            context.write(rowid, kv);
            kv = new KeyValue(rowid.get(),
                    Bytes.toBytes("DestinationStat"),
                    Bytes.toBytes("daily_total_distance"),
                    Bytes.toBytes(tokens[3]));
            context.write(rowid, kv);
            kv = new KeyValue(rowid.get(),
                    Bytes.toBytes("DestinationStat"),
                    Bytes.toBytes("daily_avg_distance"),
                    Bytes.toBytes(tokens[5]));
            context.write(rowid, kv);
            kv = new KeyValue(rowid.get(),
                    Bytes.toBytes("DestinationStat"),
                    Bytes.toBytes("daily_avg_duration"),
                    Bytes.toBytes(tokens[6]));
            context.write(rowid, kv);

        }
    }
}
