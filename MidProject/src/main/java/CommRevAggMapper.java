import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class CommRevAggMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        String tokens[] = value.toString().trim().split(",");
        if (tokens.length == 7){
            context.write(new Text(tokens[0]), new Text(tokens[2] + ":" + tokens[4]));
        }
    }
}
